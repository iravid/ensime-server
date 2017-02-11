// Copyright: 2010 - 2017 https://github.com/ensime/ensime-server/graphs
// License: http://www.gnu.org/licenses/gpl-3.0.en.html
package org.ensime.core

import akka.actor._
import akka.event.LoggingReceive
import akka.pattern.pipe
import org.ensime.api._
import org.ensime.indexer.SearchService
import org.ensime.indexer.database.DatabaseService.FqnSymbol
import org.ensime.model._
import org.ensime.vfs._

import scala.concurrent.Future

// only used for queries by other components
final case class TypeCompletionsReq(prefix: String, maxResults: Int)

class Indexer(
    index: SearchService,
    implicit val config: EnsimeConfig,
    implicit val vfs: EnsimeVFS
) extends Actor with ActorLogging {

  import context.dispatcher

  private def typeResult(hit: FqnSymbol) = TypeSearchResult(
    hit.fqn, hit.fqn.split("\\.").last, hit.declAs,
    LineSourcePositionHelper.fromFqnSymbol(hit)(config, vfs)
  )

  def oldSearchTypes(query: String, max: Int): Future[List[TypeSearchResult]] =
    for {
      searchResult <- index.searchClasses(query, max)
      filtered = searchResult.filterNot {
        name => name.fqn.endsWith("$") || name.fqn.endsWith("$class")
      }
      typed = filtered map typeResult
    } yield typed

  def oldSearchSymbols(terms: List[String], max: Int): Future[List[SymbolSearchResult]] =
    index.searchClassesMethods(terms, max).map(_.flatMap {
      case hit if hit.declAs == DeclaredAs.Class => Some(typeResult(hit))
      case hit if hit.declAs == DeclaredAs.Method => Some(MethodSearchResult(
        hit.fqn, hit.fqn.split("\\.").last, hit.declAs,
        LineSourcePositionHelper.fromFqnSymbol(hit)(config, vfs),
        hit.fqn.split("\\.").init.mkString(".")
      ))
      case _ => None // were never supported
    })

  override def receive = LoggingReceive {
    case ImportSuggestionsReq(file, point, names, maxResults) =>
      val suggestions = Future
        .traverse(names)(oldSearchTypes(_, maxResults))
        .map(ImportSuggestions)

      suggestions pipeTo sender()

    case PublicSymbolSearchReq(keywords, maxResults) =>
      val suggestions = oldSearchSymbols(keywords, maxResults).map(SymbolSearchResults)
      suggestions pipeTo sender()

    case TypeCompletionsReq(query: String, maxResults: Int) =>
      val completions = oldSearchTypes(query, maxResults).map(SymbolSearchResults)
      completions pipeTo sender()
  }
}
object Indexer {
  def apply(index: SearchService)(implicit config: EnsimeConfig, vfs: EnsimeVFS): Props = Props(classOf[Indexer], index, config, vfs)
}
