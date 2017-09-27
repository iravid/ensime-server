// Copyright: 2010 - 2017 https://github.com/ensime/ensime-server/graphs
// License: http://www.gnu.org/licenses/gpl-3.0.en.html
package org.ensime.core

import fs2.Strategy
import java.net.URI
import java.nio.file.Paths

import akka.actor._
import akka.event.LoggingReceive
import akka.pattern.pipe
import org.ensime.api._
import org.ensime.indexer.SearchService
import org.ensime.indexer.graph._
import org.ensime.model._
import org.ensime.util.ensimefile._
import org.ensime.vfs._

// only used for queries by other components
final case class TypeCompletionsReq(prefix: String, maxResults: Int)

class Indexer(
  index: SearchService,
  implicit val config: EnsimeConfig,
  implicit val vfs: EnsimeVFS
) extends Actor
    with ActorLogging {

  implicit val strategy: Strategy =
    Strategy.fromExecutionContext(context.dispatcher)

  private def typeResult(hit: FqnSymbol) = TypeSearchResult(
    hit.fqn,
    hit.fqn.split("\\.").last,
    hit.declAs,
    LineSourcePositionHelper.fromFqnSymbol(hit)(vfs)
  )

  def oldSearchTypes(query: String, max: Int): List[TypeSearchResult] = {
    // Remove $/$class from the end.
    def strip(fqn: String): String = fqn.replaceAll("\\$(class)*$", "")

    import org.ensime.util.list._

    index
      .searchClasses(query, max)
      .unsafeRun()
      .map {
        case c: ClassDef => c.copy(fqn = strip(c.fqn))
        case f: Field    => f.copy(fqn = strip(f.fqn))
        case m: Method   => m.copy(fqn = strip(m.fqn))
      }
      .distinctBy(_.fqn)
      .map(typeResult)
  }

  private val typeDecls: Set[DeclaredAs] =
    Set(DeclaredAs.Class, DeclaredAs.Trait, DeclaredAs.Object)
  def oldSearchSymbols(terms: List[String], max: Int) =
    index.searchClassesMethods(terms, max).unsafeRun().flatMap {
      case hit if typeDecls.contains(hit.declAs) => Some(typeResult(hit))
      case hit if hit.declAs == DeclaredAs.Method =>
        Some(
          MethodSearchResult(
            hit.fqn,
            hit.fqn.split("\\.").last,
            hit.declAs,
            LineSourcePositionHelper.fromFqnSymbol(hit)(vfs),
            hit.fqn.split("\\.").init.mkString(".")
          )
        )
      case _ => None // were never supported
    }

  override def receive = LoggingReceive {
    case ImportSuggestionsReq(file, point, names, maxResults) =>
      val suggestions = names.map(oldSearchTypes(_, maxResults))
      sender ! ImportSuggestions(suggestions)

    case PublicSymbolSearchReq(keywords, maxResults) =>
      val suggestions = oldSearchSymbols(keywords, maxResults)
      sender ! SymbolSearchResults(suggestions)

    case TypeCompletionsReq(query: String, maxResults: Int) =>
      sender ! SymbolSearchResults(oldSearchTypes(query, maxResults))

    case FindUsages(fqn: String) =>
      import context.dispatcher
      val usages = index.findUsageLocations(fqn)
      val response = usages.map { usages =>
        val positions: List[LineSourcePosition] = usages
          .take(1000)
          .flatMap { u =>
            val file = u.file
            file.map { f =>
              LineSourcePosition(
                EnsimeFile(Paths.get(new URI(f)).toString),
                u.line.getOrElse(0)
              )
            }
          }(collection.breakOut)
        val files = positions.foldLeft[Set[EnsimeFile]](Set.empty) {
          (files, pos) =>
            if (pos.line > 0) files + pos.file else files
        }
        val contents: Map[EnsimeFile, Array[String]] =
          files.map(f => f -> f.readAllLines.toArray)(collection.breakOut)
        val positionHints: List[PositionHint] = positions.map(
          pos =>
            PositionHint(pos, contents.get(pos.file) match {
              case Some(content) if pos.line > 0 =>
                Some(content(pos.line - 1).trim)
              case _ =>
                None
            })
        )
        SourcePositions(positionHints)
      }
      pipe(response.unsafeRunAsyncFuture()) to sender

    case FindHierarchy(fqn: String) =>
      import context.dispatcher

      def toClassInfos(h: Hierarchy): List[ClassInfo] = {
        def toClassInfo(c: ClassDef) =
          ClassInfo(c.scalaName,
                    c.fqn,
                    c.declAs,
                    LineSourcePositionHelper.fromFqnSymbol(c))
        h match {
          case TypeHierarchy(aClass, typeRefs) =>
            typeRefs.foldLeft(toClassInfo(aClass) :: Nil)(
              (listOfInfos, hierarchy) =>
                toClassInfos(hierarchy) ::: listOfInfos
            )
          case classDef: ClassDef => toClassInfo(classDef) :: Nil
        }
      }

      val ancestors = index
        .getTypeHierarchy(fqn, Hierarchy.Supertypes, None)
        .map(_.map {
          case TypeHierarchy(_, typeRefs) =>
            typeRefs.toList.flatMap(toClassInfos)
          case _ => Nil
        })
      val inheritors = index
        .getTypeHierarchy(fqn, Hierarchy.Subtypes, None)
        .map(_.map {
          case TypeHierarchy(_, typeRefs) =>
            typeRefs.toList.flatMap(toClassInfos)
          case _ => Nil
        })
      val symbolTreeInfo = for {
        anc <- ancestors
        inh <- inheritors
      } yield HierarchyInfo(anc.getOrElse(Nil), inh.getOrElse(Nil))
      pipe(symbolTreeInfo.unsafeRunAsyncFuture()) to sender
  }
}
object Indexer {
  def apply(index: SearchService)(implicit config: EnsimeConfig,
                                  vfs: EnsimeVFS): Props =
    Props(classOf[Indexer], index, config, vfs)
}
