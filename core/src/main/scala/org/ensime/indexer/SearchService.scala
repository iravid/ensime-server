// Copyright: 2010 - 2017 https://github.com/ensime/ensime-server/graphs
// License: http://www.gnu.org/licenses/gpl-3.0.en.html
package org.ensime.indexer

import fs2.{ Strategy, Stream, Task }
import java.net.URI
import org.ensime.util.Debouncer
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{ Failure, Properties, Success }
import scala.collection.{ mutable, Map, Set }

import akka.actor._
import akka.event.slf4j.SLF4JLogging
import org.apache.commons.vfs2._
import org.ensime.api._
import org.ensime.config.EnsimeConfigProtocol
import org.ensime.config.richconfig._
import org.ensime.indexer.graph._
import org.ensime.util.file._
import org.ensime.util.path._
import org.ensime.util.map._
import org.ensime.util.fileobject._
import org.ensime.vfs._

/**
 * Provides methods to perform ENSIME-specific indexing tasks,
 * receives events that require an index update, and provides
 * searches against the index.
 *
 * We have an H2 database for storing relational information
 * and Lucene for advanced indexing.
 */
class SearchService(
  config: EnsimeConfig,
  resolver: SourceResolver
)(
  implicit
  actorSystem: ActorSystem,
  serverConfig: EnsimeServerConfig,
  val vfs: EnsimeVFS
) extends FileChangeListener
    with SLF4JLogging {
  import SearchService._
  import ExecutionContext.Implicits.global // not used for heavy lifting (indexing, graph or lucene)

  private[indexer] val allTargets = config.targets.map(vfs.vfile)

  private[indexer] def isUserFile(file: FileName): Boolean =
    allTargets.exists(file isAncestor _.getName)

  private val QUERY_TIMEOUT = 30 seconds

  /**
   * Changelog:
   *
   * 2.0.4 - find usages and show implementations using Indexer
   *
   * 2.0.3g - graphpocalypse
   *
   * 2.0.3 - added JDI source information
   *
   * 2.0.2 - bump lucene and h2 versions
   *
   * 2.0.1 - change the lucene analyser
   *
   * 2.0 - upgrade Lucene, format not backwards compatible.
   *
   * 1.4 - remove redundant descriptors, doh!
   *
   * 1.3g - use a graph database
   *
   * 1.3 - methods include descriptors in (now unique) FQNs
   *
   * 1.2 - added foreign key to FqnSymbols.file with cascade delete
   *
   * 1.0 - reverted index due to negative impact to startup time. The
   *       workaround to large scale deletions is to just nuke the
   *       .ensime_cache.
   *
   * 1.1 - added index to FileCheck.file to speed up delete.
   *
   * 1.0 - initial schema
   */
  private val version = "2.0.4"

  private[indexer] val index = new IndexService(
    config.cacheDir.file / ("index-" + version)
  )
  private val db = new GraphService(
    (config.cacheDir.file / ("graph-" + version)).toFile
  )

  val noReverseLookups: Boolean =
    Properties.propOrFalse("ensime.index.no.reverse.lookups")

  private[indexer] def getTopLevelClassFile(f: FileObject): FileObject = {
    import scala.reflect.NameTransformer
    val filename  = f.getName
    val className = filename.getBaseName
    val baseClassName =
      if (className.contains("$"))
        NameTransformer.encode(
          NameTransformer.decode(className).split("\\$")(0)
        ) + ".class"
      else className
    val uri = filename.uriString
    vfs.vfile(
      uri.substring(0, uri.length - new URI(className).toASCIIString.length) + new URI(
        baseClassName
      ).toASCIIString
    )
  }

  private def scanGrouped(
    f: FileObject
  ): Task[Map[FileName, Set[FileObject]]] = Task.delay {
    val results = new mutable.HashMap[FileName, mutable.Set[FileObject]]
    with mutable.MultiMap[FileName, FileObject]
    f.findFiles(ClassfileSelector) match {
      case null => results
      case res =>
        for (fo <- res) {
          val key = getTopLevelClassFile(fo)
          if (key.exists()) {
            results.addBinding(key.getName, fo)
          }
        }
        results
    }
  }

  /**
   * Indexes everything, making best endeavours to avoid scanning what
   * is unnecessary (e.g. we already know that a jar or classfile has
   * been indexed).
   *
   * @return the number of rows (removed, indexed) from the database.
   */
  def refresh()(implicit S: Strategy): Task[(Int, Int)] = {
    // it is much faster during startup to obtain the full list of
    // known files from the DB then and check against the disk, than
    // check each file against DatabaseService.outOfDate
    def findStaleFileChecks(checks: Seq[FileCheck]): Task[List[FileCheck]] =
      Task.delay(
        checks.filter(c => !c.file.exists || c.changed).toList
      )

    // delete the stale data before adding anything new
    // returns number of rows deleted
    def deleteReferences(
      checks: List[FileCheck],
      batchSize: Int = 1000
    )(implicit S: Strategy): Task[Int] =
      for {
        _ <- Task.delay(
              log.debug(s"removing ${checks.size} stale files from the index")
            )
        deleted <- Stream
                    .emits(checks)
                    .map(_.file)
                    .chunkN(batchSize)
                    .evalMap(c => delete(c.flatMap(_.toList)))
                    .runFold(0)(_ + _)
      } yield deleted

    // a snapshot of everything that we want to index
    def findBases(): Task[(Set[FileObject], Map[FileName, Set[FileObject]])] =
      for {
        partitioned <- Task.delay {
                        config.projects.flatMap {
                          case m =>
                            m.targets
                              .map(_.file.toFile)
                              .filter(_.exists())
                              .toList ::: m.libraryJars.toList.map(
                              _.file.toFile
                            )
                        }.partition(_.isJar)
                      }

        (jarFiles, dirs) = partitioned

        grouped <- Task
                    .traverse(dirs) { d =>
                      scanGrouped(vfs.vfile(d))
                    }
                    .map(
                      _.fold(Map.empty[FileName, Set[FileObject]])(_ merge _)
                    )

        jars <- Task.delay {
                 (jarFiles ++ EnsimeConfigProtocol.javaRunTime(config))
                   .map(vfs.vfile)(collection.breakOut)
                   .toSet
               }
      } yield (jars, grouped)

    def indexBase(
      baseName: FileName,
      fileCheck: Option[FileCheck],
      grouped: Map[FileName, Set[FileObject]]
    )(implicit S: Strategy): Task[Int] =
      for {
        base       <- Task.delay(vfs.vfile(baseName.uriString))
        outOfDate  <- Task.delay(fileCheck.forall(_.changed))
        irrelevant <- Task.delay(!outOfDate || !base.exists())
        result <- if (irrelevant) Task.now(0)
                 else
                   for {
                     symbols <- extractSymbolsFromClassOrJar(base, grouped)
                     persisted <- persist(symbols,
                                          commitIndex = false,
                                          boost = isUserFile(baseName))
                     _ <- Task.delay {
                           if (base.getName.getExtension == "jar") {
                             log.debug(s"finished indexing $base")
                           }
                         }
                   } yield persisted
      } yield result

    // index all the given bases and return number of rows written
    def indexBases(jars: Set[FileObject],
                   classFiles: Map[FileName, Set[FileObject]],
                   checks: Seq[FileCheck]): Task[Int] = {
      log.debug("Indexing bases...")

      val checksLookup: Map[String, FileCheck] =
        checks.map(check => (check.filename -> check)).toMap

      val jarsWithChecks = jars.map { jar =>
        val name = jar.getName
        (name, checksLookup.get(jar.uriString))
      }

      val basesWithChecks: Seq[(FileName, Option[FileCheck])] = classFiles.map {
        case (outerClassFile, _) =>
          (outerClassFile, checksLookup.get(outerClassFile.uriString))
      }(collection.breakOut)

      val indexingTasks =
        Stream.emits((jarsWithChecks ++ basesWithChecks).toSeq).map {
          case (file, check) => Stream.eval(indexBase(file, check, classFiles))
        }

      fs2.concurrent
        .join(serverConfig.indexBatchSize)(indexingTasks)
        .runFold(0)(_ + _)
    }

    for {
      checks             <- Task.fromFuture(db.knownFiles())
      stale              <- findStaleFileChecks(checks)
      deletes            <- deleteReferences(stale)
      bases              <- findBases()
      (jars, classFiles) = bases
      added              <- indexBases(jars, classFiles, checks)
      _                  <- Task.fromFuture(index.commit())
    } yield (deletes, added)
  }

  def refreshResolver(): Unit = resolver.update()

  def persist(symbols: List[SourceSymbolInfo],
              commitIndex: Boolean,
              boost: Boolean)(implicit S: Strategy): Task[Int] = {
    val iwork = Task.fromFuture(index.persist(symbols, commitIndex, boost))
    val dwork = Task.fromFuture(db.persist(symbols))

    for {
      _       <- iwork
      inserts <- dwork
    } yield inserts
  }

  def extractSymbolsFromClassOrJar(
    file: FileObject,
    grouped: Map[FileName, Set[FileObject]]
  ): Task[List[SourceSymbolInfo]] =
    file match {
      case classfile if classfile.getName.getExtension == "class" =>
        Stream
          .bracket(Task.delay((grouped(classfile.getName), classfile)))(
            {
              case (files, classfile) =>
                Stream.eval(extractSymbols(classfile, files, classfile))
            }, {
              case (files, classfile) =>
                Task.delay {
                  files.foreach(_.close())
                  classfile.close()
                }
            }
          )
          .runFold(List.empty[SourceSymbolInfo])(_ ++ _)

      case jar =>
        for {
          _ <- Task.delay(log.debug(s"indexing $jar"))
          symbols <- Stream
                      .bracket(Task.delay(vfs.vjar(jar.asLocalFile)))(
                        vJar =>
                          Stream.eval {
                            for {
                              grouped <- scanGrouped(vJar)
                              symbols <- Task.traverse(grouped.toSeq) {
                                          case (root, files) =>
                                            extractSymbols(
                                              jar,
                                              files,
                                              vfs.vfile(root.uriString)
                                            )
                                        }
                            } yield symbols.flatten.toList
                        },
                        vJar => Task.delay(vfs.nuke(vJar))
                      )
                      .runFold(List.empty[SourceSymbolInfo])(_ ++ _)
        } yield symbols
    }

  private val blacklist = Set("sun/", "sunw/", "com/sun/")
  private val ignore    = Set("$$", "$worker$")

  private def extractSymbols(
    container: FileObject,
    files: collection.Set[FileObject],
    rootClassFile: FileObject
  ): Task[List[SourceSymbolInfo]] = Task.delay {
    def getInternalRefs(isUserFile: Boolean,
                        s: RawSymbol): List[FullyQualifiedReference] =
      if (isUserFile && !noReverseLookups) s.internalRefs else List.empty

    val depickler     = new ClassfileDepickler(rootClassFile)
    val scalapClasses = depickler.getClasses

    val res: List[SourceSymbolInfo] = files.flatMap {
      case f
          if f.pathWithinArchive.exists(
            relative => blacklist.exists(relative.startsWith)
          ) =>
        List(EmptySourceSymbolInfo(FileCheck(container)))
      case f =>
        val path = f.uriString
        val file = if (path.startsWith("jar") || path.startsWith("zip")) {
          FileCheck(container)
        } else FileCheck(f)
        val indexer = new ClassfileIndexer(f)
        val clazz   = indexer.indexClassfile()

        val userFile = isUserFile(f.getName)
        val source   = resolver.resolve(clazz.name.pack, clazz.source)

        val sourceUri = source.map(_.uriString)

        val jdi = source.map { src =>
          val pkg = clazz.name.pack.path.mkString("/")
          s"$pkg/${src.getName.getBaseName}"
        }

        val scalapClassInfo = scalapClasses.get(clazz.name.fqnString)

        scalapClassInfo match {
          case _ if clazz.access != Public            => List(EmptySourceSymbolInfo(file))
          case _ if ignore.exists(clazz.fqn.contains) => Nil
          case Some(scalapSymbol) =>
            val classInfo = ClassSymbolInfo(file,
                                            path,
                                            sourceUri,
                                            getInternalRefs(userFile, clazz),
                                            clazz,
                                            Some(scalapSymbol),
                                            jdi)

            val fields = clazz.fields.map(
              f =>
                FieldSymbolInfo(file,
                                sourceUri,
                                getInternalRefs(userFile, f),
                                f,
                                scalapSymbol.fields.get(f.fqn))
            )

            val methods = clazz.methods.groupBy(_.name.name).flatMap {
              case (methodName, overloads) =>
                val scalapMethods = scalapSymbol.methods.get(methodName)
                overloads.iterator.zipWithIndex.map {
                  case (m, i) =>
                    val scalap = scalapMethods.fold(
                      Option.empty[RawScalapMethod]
                    )(seq => if (seq.length <= i) None else Some(seq(i)))
                    MethodSymbolInfo(file,
                                     sourceUri,
                                     getInternalRefs(userFile, m),
                                     m,
                                     scalap)
                }
            }

            val aliases = scalapSymbol.typeAliases.valuesIterator
              .map(alias => TypeAliasSymbolInfo(file, sourceUri, alias))
              .toList

            classInfo :: fields ::: methods.toList ::: aliases
          case None =>
            val cl = ClassSymbolInfo(file,
                                     path,
                                     sourceUri,
                                     getInternalRefs(userFile, clazz),
                                     clazz,
                                     None,
                                     jdi)
            val methods: List[MethodSymbolInfo] = clazz.methods.map(
              m =>
                MethodSymbolInfo(file,
                                 sourceUri,
                                 getInternalRefs(userFile, m),
                                 m,
                                 None)
            )(collection.breakOut)
            val fields = clazz.fields.map(
              f =>
                FieldSymbolInfo(file,
                                sourceUri,
                                getInternalRefs(userFile, f),
                                f,
                                None)
            )
            cl :: methods ::: fields
        }
    }(collection.breakOut)
    res.filterNot(sym => ignore.exists(sym.fqn.contains)).sortWith {
      case (cl1: ClassSymbolInfo, cl2: ClassSymbolInfo) => cl1.fqn < cl2.fqn
      case (cl: ClassSymbolInfo, _)                     => true
      case _                                            => false
    }
  }

  /** free-form search for classes */
  def searchClasses(query: String,
                    max: Int)(implicit S: Strategy): Task[List[FqnSymbol]] =
    for {
      fqns    <- Task.fromFuture(index.searchClasses(query, max))
      results <- Task.fromFuture(db.find(fqns))
    } yield results take max

  /** free-form search for classes and methods */
  def searchClassesMethods(terms: List[String], max: Int)(
    implicit S: Strategy
  ): Task[List[FqnSymbol]] =
    for {
      fqns    <- Task.fromFuture(index.searchClassesMethods(terms, max))
      results <- Task.fromFuture(db.find(fqns))
    } yield results take max

  /** only for exact fqns */
  def findUnique(fqn: String)(implicit S: Strategy): Task[Option[FqnSymbol]] =
    Task.fromFuture(db.find(fqn))

  /** returns hierarchy of a type identified by fqn */
  def getTypeHierarchy(
    fqn: String,
    hierarchyType: Hierarchy.Direction,
    levels: Option[Int] = None
  )(implicit S: Strategy): Task[Option[Hierarchy]] =
    Task.fromFuture(db.getClassHierarchy(fqn, hierarchyType, levels))

  /** returns locations where given fqn is referred*/
  def findUsageLocations(
    fqn: String
  )(implicit S: Strategy): Task[Iterable[UsageLocation]] =
    Task.fromFuture(db.findUsageLocations(fqn))

  /** returns FqnSymbols where given fqn is referred*/
  def findUsages(fqn: String)(implicit S: Strategy): Task[Iterable[FqnSymbol]] =
    Task.fromFuture(db.findUsages(fqn))

  // blocking badness
  def findClasses(file: EnsimeFile)(implicit S: Strategy): Task[Seq[ClassDef]] =
    Task.fromFuture(db.findClasses(file))
  def findClasses(jdi: String)(implicit S: Strategy): Task[Seq[ClassDef]] =
    Task.fromFuture(db.findClasses(jdi))

  /* DELETE then INSERT in H2 is ridiculously slow, so we put all modifications
   * into a blocking queue and dedicate a thread to block on draining the queue.
   * This has the effect that we always react to a single change on disc but we
   * will work through backlogs in bulk.
   *
   * We always do a DELETE, even if the entries are new, but only INSERT if
   * the list of symbols is non-empty.
   */

  val backlogActor =
    actorSystem.actorOf(Props(new IndexingQueueActor(this)), "ClassfileIndexer")

  // returns number of rows removed
  def delete(files: List[FileObject])(implicit S: Strategy): Task[Int] = {
    // this doesn't speed up Lucene deletes, but it means that we
    // don't wait for Lucene before starting the H2 deletions.
    val iwork = Task.fromFuture(index.remove(files))
    val dwork = Task.fromFuture(db.removeFiles(files))

    for {
      iwork    <- Task.start(iwork)
      dwork    <- Task.start(dwork)
      _        <- iwork
      removals <- dwork
    } yield removals
  }

  def fileChanged(f: FileObject): Unit = backlogActor ! IndexFile(f)
  def fileRemoved(f: FileObject): Unit = fileChanged(f)
  def fileAdded(f: FileObject): Unit   = fileChanged(f)

  def shutdown()(implicit S: Strategy): Task[Unit] =
    for {
      _ <- Task.fromFuture(db.shutdown())
      _ <- Task.fromFuture(index.shutdown())
    } yield ()
}

object SearchService {
  sealed trait SourceSymbolInfo {
    def file: FileCheck
    def fqn: String
    def internalRefs: List[FullyQualifiedReference]
    def scalapSymbol: Option[RawScalapSymbol]
  }

  final case class EmptySourceSymbolInfo(
    file: FileCheck
  ) extends SourceSymbolInfo {
    override def fqn: String                                 = ""
    override def internalRefs: List[FullyQualifiedReference] = List.empty
    override def scalapSymbol: Option[RawScalapSymbol]       = None
  }

  final case class ClassSymbolInfo(
    file: FileCheck,
    path: String,
    source: Option[String],
    internalRefs: List[FullyQualifiedReference],
    bytecodeSymbol: RawClassfile,
    scalapSymbol: Option[RawScalapClass],
    jdi: Option[String]
  ) extends SourceSymbolInfo {
    override def fqn: String = bytecodeSymbol.fqn
  }

  final case class MethodSymbolInfo(
    file: FileCheck,
    source: Option[String],
    internalRefs: List[FullyQualifiedReference],
    bytecodeSymbol: RawMethod,
    scalapSymbol: Option[RawScalapMethod]
  ) extends SourceSymbolInfo {
    override def fqn: String = bytecodeSymbol.fqn
  }

  final case class FieldSymbolInfo(
    file: FileCheck,
    source: Option[String],
    internalRefs: List[FullyQualifiedReference],
    bytecodeSymbol: RawField,
    scalapSymbol: Option[RawScalapField]
  ) extends SourceSymbolInfo {
    override def fqn: String = bytecodeSymbol.fqn
  }

  final case class TypeAliasSymbolInfo(
    file: FileCheck,
    source: Option[String],
    t: RawType
  ) extends SourceSymbolInfo {
    override def scalapSymbol: Option[RawScalapSymbol]       = Some(t)
    override def fqn: String                                 = t.javaName.fqnString
    override def internalRefs: List[FullyQualifiedReference] = List.empty
  }
}

final case class IndexFile(f: FileObject)

class IndexingQueueActor(searchService: SearchService)
    extends Actor
    with ActorLogging {
  import scala.concurrent.duration._
  implicit val S: Strategy = Strategy.fromExecutionContext(context.dispatcher)

  case object Process

  // De-dupes files that have been updated since we were last told to
  // index them. No need to aggregate values: the latest wins. Key is
  // the URI because FileObject doesn't implement equals
  private val todo = new mutable.HashMap[FileName, mutable.Set[FileObject]]
  with mutable.MultiMap[FileName, FileObject]

  private val advice =
    "If the problem persists, you may need to restart ensime."

  val processDebounce =
    Debouncer.forActor(self, Process, delay = 5.seconds, maxDelay = 1.hour)

  override def receive: Receive = {
    case IndexFile(f) =>
      val topLevelClassFile = f match {
        case jar if jar.getName.getExtension == "jar" => jar
        case classFile                                => searchService.getTopLevelClassFile(classFile)
      }
      todo.addBinding(topLevelClassFile.getName, topLevelClassFile)
      processDebounce.call()

    case Process if todo.isEmpty => // nothing to do

    case Process =>
      val batch = todo.take(250)
      batch.keys.foreach(todo.remove)
      if (todo.nonEmpty)
        processDebounce.call()

      import ExecutionContext.Implicits.global

      log.debug(s"Indexing ${batch.size} groups of files")

      def retry(): Unit =
        batch.valuesIterator.foreach(_.foreach(self ! IndexFile(_)))

      batch
        .grouped(10)
        .foreach(
          chunk =>
            Future
              .sequence(chunk.map {
                case (outerClassFile, _) =>
                  val filename = outerClassFile.getPath
                  // I don't trust VFS's f.exists()
                  if (!File(filename).exists()) {
                    Future.successful(outerClassFile -> Nil)
                  } else
                    searchService
                      .extractSymbolsFromClassOrJar(
                        searchService.vfs.vfile(outerClassFile.uriString),
                        batch
                      )
                      .map(outerClassFile ->)
                      .unsafeRunAsyncFuture()
              })
              .onComplete {
                case Failure(t) =>
                  log.error(
                    t,
                    s"failed to index batch of ${batch.size} files. $advice"
                  )
                  retry()
                case Success(indexed) =>
                  searchService
                    .delete(
                      indexed.flatMap(f => batch(f._1))(collection.breakOut)
                    )
                    .unsafeRunAsyncFuture()
                    .onComplete {
                      case Failure(t) =>
                        log.error(
                          t,
                          s"failed to remove stale entries in ${batch.size} files. $advice"
                        )
                        retry()
                      case Success(_) =>
                        indexed.foreach {
                          case (file, syms) =>
                            val boost = searchService.isUserFile(file)
                            val persisting = searchService
                              .persist(syms, commitIndex = true, boost = boost)
                              .unsafeRunAsyncFuture()

                            persisting.onComplete {
                              case Failure(t) =>
                                log.error(
                                  t,
                                  s"failed to persist entries in $file. $advice"
                                )
                                retry()
                              case Success(_) =>
                            }
                        }
                    }

            }
        )
  }

}
