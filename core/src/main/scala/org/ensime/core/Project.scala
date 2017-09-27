// Copyright: 2010 - 2017 https://github.com/ensime/ensime-server/graphs
// License: http://www.gnu.org/licenses/gpl-3.0.en.html
package org.ensime.core

import fs2.Strategy
import scala.concurrent.duration._
import scala.util._

import akka.actor._
import org.apache.commons.vfs2.FileObject
import org.ensime.api._
import org.ensime.config.richconfig._
import org.ensime.core.debug.DebugActor
import org.ensime.indexer._
import org.ensime.util.{ Debouncer, Timing }
import org.ensime.util.FileUtils._
import org.ensime.util.ensimefile._
import org.ensime.vfs._

final case class ShutdownRequest(reason: String, isError: Boolean = false)

/**
 * The Project actor simply forwards messages coming from the user to
 * the respective subcomponent.
 */
class Project(
  broadcaster: ActorRef,
  implicit val config: EnsimeConfig,
  implicit val serverConfig: EnsimeServerConfig
) extends Actor
    with ActorLogging
    with Stash {
  import context.system
  implicit val strategy: Strategy =
    Strategy.fromExecutionContext(context.dispatcher)

  /* The main components of the ENSIME server */
  private var scalac: ActorRef   = _
  private var javac: ActorRef    = _
  private var debugger: ActorRef = _

  private var indexer: ActorRef = _
  private var docs: ActorRef    = _

  // LEGACY: holds messages until clients expect them
  var delayedBroadcaster: ActorRef = _

  // vfs, resolver, search and watchers are considered "reliable" (hah!)
  private implicit val vfs: EnsimeVFS = EnsimeVFS()
  private val resolver                = new SourceResolver(config)
  private val searchService           = new SearchService(config, resolver)

  private var dependentProjects: Map[EnsimeProjectId, List[EnsimeProjectId]] = _

  private val reTypecheck = new FileChangeListener {
    private val askReTypeCheck: Map[EnsimeProjectId, Debouncer] =
      config.projects.map(
        p =>
          p.id -> Debouncer.forActor(
            self,
            RestartScalaCompilerReq(Some(p.id), ReloadStrategy.KeepLoaded),
            delay = (5 * Timing.dilation).seconds,
            maxDelay = (20 * Timing.dilation).seconds
        )
      )(collection.breakOut)
    def fileChanged(f: FileObject): Unit = {
      val projectId = config.findProject(f)
      projectId foreach { projectId =>
        (projectId :: dependentProjects.getOrElse(projectId, Nil))
          .foreach(askReTypeCheck.get(_).foreach(_.call()))
      }
    }
    def fileAdded(f: FileObject): Unit =
      fileChanged(f)
    def fileRemoved(f: FileObject): Unit =
      fileChanged(f)
    override def baseReCreated(f: FileObject): Unit =
      askReTypeCheck.values.foreach(_.call())
  }
  context.actorOf(
    Props(new ClassfileWatcher(searchService :: reTypecheck :: Nil)),
    "classFileWatcher"
  )

  override def preStart(): Unit = {
    dependentProjects = config.projects.map(
      p => p.id -> config.projects.filter(_.depends.contains(p.id)).map(_.id)
    )(collection.breakOut)
    searchService
      .refresh()
      .unsafeRunAsyncFuture()
      .onComplete {
        case Success((deletes, inserts)) =>
          broadcaster ! Broadcaster.Persist(IndexerReadyEvent)
          log.debug(s"created $inserts and removed $deletes searchable rows")
          if (serverConfig.exitAfterIndex)
            context.parent ! ShutdownRequest("Index only run", isError = false)
        case Failure(problem) =>
          log.warning(s"Refresh failed: ${problem.toString}")
          throw problem
      }(context.dispatcher)

    indexer = context.actorOf(Indexer(searchService), "indexer")
    if (config.scalaLibrary.isDefined || Set("scala", "dotty")(config.name)) {
      scalac = context.actorOf(
        AnalyzerManager(broadcaster,
                        Analyzer(broadcaster, indexer, searchService, _)),
        "scalac"
      )
      javac = context.actorOf(JavaAnalyzer(broadcaster, indexer, searchService),
                              "javac")
    } else {
      log.warning(
        "Detected a pure Java project. Scala queries are not available."
      )
      scalac = system.deadLetters
      javac = context.actorOf(JavaAnalyzer(broadcaster, indexer, searchService),
                              "javac")
    }
    debugger =
      context.actorOf(DebugActor.props(broadcaster, searchService), "debugging")
    docs = context.actorOf(DocResolver(), "docs")

    broadcaster ! Broadcaster.Persist(GreetingInfo())
  }

  override def postStop(): Unit = {
    // make sure the "reliable" dependencies are cleaned up
    Try(searchService.shutdown().unsafeRun())
    Try(vfs.close())
  }

  // debounces compiler restarts
  private var rechecking: Cancellable = _

  def receive: Receive = {
    case ShutdownRequest                              => context.parent forward ShutdownRequest
    case req @ RestartScalaCompilerReq(_, _)          => scalac forward req
    case m @ TypecheckFileReq(sfi) if sfi.file.isJava => javac forward m
    case m @ CompletionsReq(sfi, _, _, _, _) if sfi.file.isJava =>
      javac forward m
    case m @ DocUriAtPointReq(sfi, _) if sfi.file.isJava => javac forward m
    case m @ TypeAtPointReq(sfi, _) if sfi.file.isJava   => javac forward m
    case m @ SymbolDesignationsReq(sfi, _, _, _) if sfi.file.isJava =>
      javac forward m
    case m @ SymbolAtPointReq(sfi, _) if sfi.file.isJava => javac forward m

    // mixed mode query
    case TypecheckFilesReq(files) =>
      val (javas, scalas) = files.partition(_.file.isJava)
      if (javas.nonEmpty) javac forward TypecheckFilesReq(javas)
      if (scalas.nonEmpty) scalac forward TypecheckFilesReq(scalas)

    case m: RpcAnalyserRequest => scalac forward m
    case m: RpcDebuggerRequest => debugger forward m
    case m: RpcSearchRequest   => indexer forward m
    case m: DocSigPair         => docs forward m

    // added here to prevent errors when client sends this repeatedly (e.g. as a keepalive
    case ConnectionInfoReq => sender() ! ConnectionInfo()
  }

}

object Project {
  def apply(target: ActorRef)(implicit config: EnsimeConfig,
                              serverConfig: EnsimeServerConfig): Props =
    Props(classOf[Project], target, config, serverConfig)
}
