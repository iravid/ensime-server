// Copyright: 2010 - 2017 https://github.com/ensime/ensime-server/graphs
// License: http://www.gnu.org/licenses/gpl-3.0.en.html
package org.ensime.core

import akka.actor._
import akka.event.LoggingReceive.withLabel
import org.apache.commons.vfs2.FileObject
import org.ensime.api._
import org.ensime.core.debug.DebugActor
import org.ensime.vfs._
import org.ensime.indexer._

import scala.collection.immutable.ListSet
import scala.concurrent.duration._
import scala.util.Properties._
import scala.util._
import org.ensime.util.FileUtils
import org.ensime.util.ensimefile._

final case class ShutdownRequest(reason: String, isError: Boolean = false)

/**
 * The Project actor simply forwards messages coming from the user to
 * the respective subcomponent.
 */
class Project(
    broadcaster: ActorRef,
    implicit val config: EnsimeConfig
) extends Actor with ActorLogging with Stash {
  import context.{ dispatcher, system }

  import FileUtils._

  /* The main components of the ENSIME server */
  private var scalac: ActorRef = _
  private var javac: ActorRef = _
  private var debugger: ActorRef = _

  private var indexer: ActorRef = _
  private var docs: ActorRef = _

  // LEGACY: holds messages until clients expect them
  var delayedBroadcaster: ActorRef = _

  // vfs, resolver, search and watchers are considered "reliable" (hah!)
  private implicit val vfs: EnsimeVFS = EnsimeVFS()
  private val resolver = new SourceResolver(config)
  private val searchService = new SearchService(config, resolver)
  private val sourceWatcher = new SourceWatcher(config, resolver :: Nil)
  private val reTypecheck = new FileChangeListener {
    def reTypeCheck(): Unit = self ! AskReTypecheck
    def fileAdded(f: FileObject): Unit = reTypeCheck()
    def fileChanged(f: FileObject): Unit = reTypeCheck()
    def fileRemoved(f: FileObject): Unit = reTypeCheck()
    override def baseReCreated(f: FileObject): Unit = reTypeCheck()
  }
  private val classfileWatcher = context.actorOf(Props(new ClassfileWatcher(config, searchService :: reTypecheck :: Nil)), "classFileWatcher")

  def receive: Receive = awaitingConnectionInfoReq

  // The original ensime protocol won't send any messages to the
  // client until it has obliged a request for the connection info.
  // Although this is irrelevant now, it is expected by clients and
  // must be maintained. Ideally we'd remove this request and make
  // the response be an async message.
  def awaitingConnectionInfoReq: Receive = withLabel("awaitingConnectionInfoReq") {
    case ShutdownRequest => context.parent forward ShutdownRequest
    case ConnectionInfoReq =>
      sender() ! ConnectionInfo()
      context.become(handleRequests)
      unstashAll()
      delayedBroadcaster ! FloodGate.Activate
    case other =>
      stash()
  }

  override def preStart(): Unit = {
    delayedBroadcaster = system.actorOf(FloodGate(broadcaster), "delay")

    searchService.refresh().onComplete {
      case Success((deletes, inserts)) =>
        // legacy clients expect to see IndexerReady on connection.
        // we could also just blindly send this on each connection.
        delayedBroadcaster ! Broadcaster.Persist(IndexerReadyEvent)
        log.debug(s"created $inserts and removed $deletes searchable rows")
        if (propOrFalse("ensime.exitAfterIndex"))
          context.parent ! ShutdownRequest("Index only run", isError = false)
      case Failure(problem) =>
        log.warning(s"Refresh failed: ${problem.toString}")
        throw problem
    }(context.dispatcher)

    indexer = context.actorOf(Indexer(searchService), "indexer")
    if (config.scalaLibrary.isDefined || Set("scala", "dotty")(config.name)) {

      // we merge scala and java AnalyzerReady messages into a single
      // AnalyzerReady message, fired only after java *and* scala are ready
      val merger = context.actorOf(Props(new Actor {
        var senders = ListSet.empty[ActorRef]
        def receive: Receive = {
          case Broadcaster.Persist(AnalyzerReadyEvent) if senders.size == 1 =>
            delayedBroadcaster ! Broadcaster.Persist(AnalyzerReadyEvent)
          case Broadcaster.Persist(AnalyzerReadyEvent) => senders += sender()
          case msg => delayedBroadcaster forward msg
        }
      }))

      scalac = context.actorOf(Analyzer(merger, indexer, searchService), "scalac")
      javac = context.actorOf(JavaAnalyzer(merger, indexer, searchService), "javac")
    } else {
      log.warning("Detected a pure Java project. Scala queries are not available.")
      scalac = system.deadLetters
      javac = context.actorOf(JavaAnalyzer(delayedBroadcaster, indexer, searchService), "javac")
    }
    debugger = context.actorOf(DebugActor.props(delayedBroadcaster), "debugging")
    docs = context.actorOf(DocResolver(), "docs")
  }

  override def postStop(): Unit = {
    // make sure the "reliable" dependencies are cleaned up
    Try(sourceWatcher.shutdown())
    searchService.shutdown() // async
    Try(vfs.close())
  }

  // debounces ReloadExistingFilesEvent
  private var rechecking: Cancellable = _

  def handleRequests: Receive = withLabel("handleRequests") {
    case ShutdownRequest => context.parent forward ShutdownRequest
    case AskReTypecheck =>
      Option(rechecking).foreach(_.cancel())
      rechecking = system.scheduler.scheduleOnce(
        5 seconds, scalac, ReloadExistingFilesEvent
      )
    // HACK: to expedite initial dev, Java requests use the Scala API
    case m @ TypecheckFileReq(sfi) if sfi.file.isJava => javac forward m
    case m @ CompletionsReq(sfi, _, _, _, _) if sfi.file.isJava => javac forward m
    case m @ DocUriAtPointReq(sfi, _) if sfi.file.isJava => javac forward m
    case m @ TypeAtPointReq(sfi, _) if sfi.file.isJava => javac forward m
    case m @ SymbolDesignationsReq(sfi, _, _, _) if sfi.file.isJava => javac forward m
    case m @ SymbolAtPointReq(sfi, _) if sfi.file.isJava => javac forward m

    // mixed mode query
    case TypecheckFilesReq(files) =>
      val (javas, scalas) = files.partition(_.file.isJava)
      if (javas.nonEmpty) javac forward TypecheckFilesReq(javas)
      if (scalas.nonEmpty) scalac forward TypecheckFilesReq(scalas)

    case m: RpcAnalyserRequest => scalac forward m
    case m: RpcDebuggerRequest => debugger forward m
    case m: RpcSearchRequest => indexer forward m
    case m: DocSigPair => docs forward m

    // added here to prevent errors when client sends this repeatedly (e.g. as a keepalive
    case ConnectionInfoReq =>
      sender() ! ConnectionInfo()
  }

  override val supervisorStrategy = OneForOneStrategy() {
    case e if sender() == indexer =>
      log.error(e, "Indexer threw an error. Restarting it")
      SupervisorStrategy.Restart

    case otherwise =>
      log.error(otherwise, s"${sender()} threw an error. Escalating")
      SupervisorStrategy.Escalate
  }

}

object Project {
  def apply(target: ActorRef)(implicit config: EnsimeConfig): Props =
    Props(classOf[Project], target, config)
}
