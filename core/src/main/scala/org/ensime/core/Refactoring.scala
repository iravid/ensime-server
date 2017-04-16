// Copyright: 2010 - 2017 https://github.com/ensime/ensime-server/graphs
// License: http://www.gnu.org/licenses/gpl-3.0.en.html
package org.ensime.core

import java.nio.charset.Charset

import org.ensime.api._
import org.ensime.util.FileUtils._
import org.ensime.util._
import org.ensime.util.file.File

import scala.concurrent.{ ExecutionContext, Future }
import scala.tools.nsc.io.AbstractFile
import scala.tools.refactoring._
import scala.tools.refactoring.analysis.{ GlobalIndexes, TreeAnalysis }
import scala.tools.refactoring.common.{ Change, CompilerAccess, RenameSourceFileChange }
import scala.tools.refactoring.implementations._
import scala.tools.refactoring.transformation.TreeFactory

abstract class RefactoringEnvironment(file: String, start: Int, end: Int) {

  val refactoring: MultiStageRefactoring with CompilerAccess

  def performRefactoring(
    procId: Int,
    tpe: RefactorType,
    parameters: refactoring.RefactoringParameters
  )(implicit charset: Charset): Either[RefactorFailure, RefactorDiffEffect] = {

    def transformToDiff(modifications: List[Change]): Either[RefactorFailure, RefactorDiffEffect] = {
      val renameChange = modifications.collectFirst { case ch: RenameSourceFileChange => ch }
      val renameTarget = renameChange.map {
        ch =>
          val sourceFile = ch.sourceFile
          val fullNewName = sourceFile.path.replace(sourceFile.name, ch.to)
          (sourceFile.file, File(fullNewName))
      }
      val edits = modifications.flatMap(FileEditHelper.fromChange).sorted

      writeDiffChanges(edits, renameTarget) match {
        case Right(diff) => Right(RefactorDiffEffect(procId, tpe, diff))
        case Left(err) => Left(RefactorFailure(procId, err.toString))
      }
    }

    val af = AbstractFile.getFile(file)

    refactoring.compilationUnitOfFile(af) match {
      case Some(cu) =>
        val selection = new refactoring.FileSelection(af, cu.body, start, end)
        refactoring.prepare(selection) match {
          case Right(prepare) =>
            refactoring.perform(selection, prepare, parameters) match {
              case Right(modifications) => transformToDiff(modifications)
              case Left(error) => Left(RefactorFailure(procId, error.cause))
            }
          case Left(error) => Left(RefactorFailure(procId, error.cause))
        }
      case None =>
        Left(RefactorFailure(procId, "Compilation unit not found: " + af))
    }

  }
}

trait RefactoringHandler { self: Analyzer =>

  implicit def cs: Charset = charset

  def handleRefactorRequest(req: RefactorReq)(implicit ec: ExecutionContext): Future[RpcResponse] =
    scalaCompiler.askDoRefactor(req.procId, req.params).map {
      case Right(success) => success
      case Left(failure) => failure
    }
}

trait RefactoringControl { self: RichCompilerControl with RefactoringImpl =>

  def askDoRefactor(
    procId: Int,
    refactor: RefactorDesc
  )(
    implicit
    ec: ExecutionContext
  ): Future[Either[RefactorFailure, RefactorDiffEffect]] = doRefactor(procId, refactor)

}

trait RefactoringImpl {
  self: RichPresentationCompiler =>

  import org.ensime.util.FileUtils._

  protected def doRename(procId: Int, tpe: RefactorType, name: String, file: File, start: Int, end: Int, files: Set[String]) =
    new RefactoringEnvironment(file.getPath, start, end) {
      val refactoring = new Rename with GlobalIndexes {
        val global = RefactoringImpl.this
        val cuIndexes: List[CompilationUnitIndex] = this.global.unitOfFile.collect {
          case (f, unit) if search.noReverseLookups || files.contains(f.file.getPath) =>
            CompilationUnitIndex(unit.body)
        }(collection.breakOut)
        val index = GlobalIndex(cuIndexes)
      }
      val result = performRefactoring(procId, tpe, name)
    }.result

  protected def doExtractMethod(procId: Int, tpe: RefactorType,
    name: String, file: File, start: Int, end: Int) =
    new RefactoringEnvironment(file.getPath, start, end) {
      val refactoring = new ExtractMethod with GlobalIndexes {
        val global = RefactoringImpl.this
        val cuIndexes = this.global.activeUnits().map { u => CompilationUnitIndex(u.body) }
        val index = GlobalIndex(cuIndexes)
      }
      val result = performRefactoring(procId, tpe, name)
    }.result

  protected def doExtractLocal(procId: Int, tpe: RefactorType,
    name: String, file: File, start: Int, end: Int) =
    new RefactoringEnvironment(file.getPath, start, end) {
      val refactoring = new ExtractLocal with GlobalIndexes {
        val global = RefactoringImpl.this
        val cuIndexes = this.global.activeUnits().map { u => CompilationUnitIndex(u.body) }
        val index = GlobalIndex(cuIndexes)
      }
      val result = performRefactoring(procId, tpe, name)
    }.result

  protected def doInlineLocal(procId: Int, tpe: RefactorType, file: File,
    start: Int, end: Int) =
    new RefactoringEnvironment(file.getPath, start, end) {
      val refactoring = new InlineLocal with GlobalIndexes {
        val global = RefactoringImpl.this
        val cuIndexes = this.global.activeUnits().map { u => CompilationUnitIndex(u.body) }
        val index = GlobalIndex(cuIndexes)
      }
      val result = performRefactoring(procId, tpe, new refactoring.RefactoringParameters())
    }.result

  // we probably want to allow users to customise this
  private def organizeImportOptions(refactoring: OrganizeImports) = {
    import refactoring.oiWorker.participants._
    List(
      SortImports,
      SortImportSelectors,
      RemoveDuplicates
    )
  }

  protected def doOrganizeImports(procId: Int, tpe: RefactorType, file: File) =
    new RefactoringEnvironment(file.getPath, 0, 0) {
      val refactoring = new OrganizeImports {
        val global = RefactoringImpl.this
      }

      val result = performRefactoring(procId, tpe, new refactoring.RefactoringParameters(
        options = organizeImportOptions(refactoring),
        config = Some(OrganizeImports.OrganizeImportsConfig(
          importsStrategy = Some(OrganizeImports.ImportsStrategy.CollapseImports),
          groups = List("java", "scala")
        ))
      ))
    }.result

  protected def doAddImport(procId: Int, tpe: RefactorType, qualName: String, file: File) = {
    val refactoring = new AddImportStatement {
      val global = RefactoringImpl.this
    }

    val af = AbstractFile.getFile(file.getPath)
    val modifications = refactoring.addImport(af, qualName)
    val edits = modifications.flatMap(FileEditHelper.fromChange)

    writeDiffChanges(edits)(charset) match {
      case Right(diff) => Right(RefactorDiffEffect(procId, tpe, diff))
      case Left(err) => Left(RefactorFailure(procId, err.toString))
    }
  }

  protected def doExpandMatchCases(procId: Int, tpe: RefactorType, file: File, start: Int, end: Int): Either[RefactorFailure, RefactorDiffEffect] =
    new RefactoringEnvironment(file.getPath, start, end) {
      val refactoring = new ExpandMatchCases with GlobalIndexes {
        val global = RefactoringImpl.this
        val cuIndexes = this.global.activeUnits().map { u => CompilationUnitIndex(u.body) }
        val index = GlobalIndex(cuIndexes)
      }
      val result = performRefactoring(procId, tpe, ())
    }.result

  protected def reloadAndType(f: File) = reloadAndTypeFiles(List(this.createSourceFile(f.getPath)))

  protected def doRefactor(
    procId: Int,
    refactor: RefactorDesc
  )(
    implicit
    ec: ExecutionContext
  ): Future[Either[RefactorFailure, RefactorDiffEffect]] = {

    def askRefactorResult(
      op: => Either[RefactorFailure, RefactorDiffEffect]
    ): Future[Either[RefactorFailure, RefactorDiffEffect]] =
      Future.successful(
        askOption {
          try {
            op
          } catch {
            case e: Throwable =>
              logger.error("Error during refactor request: " + refactor, e)
              Left(RefactorFailure(procId, e.toString))
          }
        }.getOrElse(Left(RefactorFailure(procId, "Refactor call failed.")))
      )

    val tpe = refactor.refactorType

    refactor match {
      case InlineLocalRefactorDesc(file, start, end) =>
        askRefactorResult {
          reloadAndType(file)
          doInlineLocal(procId, tpe, file, start, end)
        }
      case RenameRefactorDesc(newName, file, start, end) =>
        import scala.reflect.internal.util.{ RangePosition, OffsetPosition }
        val sourceFile = createSourceFile(file.getPath)
        askLoadedTyped(sourceFile)
        val pos = if (start == end) new OffsetPosition(sourceFile, start)
        else new RangePosition(sourceFile, start, start, end)
        val symbol = askSymbolAt(pos)
        symbol match {
          case None => Future.successful(Left(RefactorFailure(procId, "No symbol at given position.")))
          case Some(sym) =>
            usesOfSym(sym).map { uses =>
              val files = uses.map(rf => createSourceFile(rf)) - sourceFile
              files.foreach(askLoadedTyped)
              askOption(doRename(procId, tpe, newName, file, start, end, files.map(_.path) + sourceFile.path))
                .getOrElse(Left(RefactorFailure(procId, "Refactor call failed.")))
            }
        }
      case ExtractMethodRefactorDesc(methodName, file, start, end) =>
        askRefactorResult {
          reloadAndType(file)
          doExtractMethod(procId, tpe, methodName, file, start, end)
        }
      case ExtractLocalRefactorDesc(name, file, start, end) =>
        askRefactorResult {
          reloadAndType(file)
          doExtractLocal(procId, tpe, name, file, start, end)
        }
      case OrganiseImportsRefactorDesc(file) =>
        askRefactorResult {
          reloadAndType(file)
          doOrganizeImports(procId, tpe, file)
        }
      case AddImportRefactorDesc(qualifiedName, file) =>
        askRefactorResult {
          reloadAndType(file)
          doAddImport(procId, tpe, qualifiedName, file)
        }
      case ExpandMatchCasesDesc(file, start, end) =>
        askRefactorResult {
          reloadAndType(file)
          doExpandMatchCases(procId, tpe, file, start, end)
        }
    }
  }
}

abstract class ExpandMatchCases extends MultiStageRefactoring
    with TreeAnalysis with analysis.Indexes with TreeFactory with common.InteractiveScalaCompiler {
  import global._

  case class PreparationResult(matchBlock: Match, selectorType: SelectorType)

  sealed trait SelectorType
  case object SingleCaseClass extends SelectorType
  case object SingleCaseObject extends SelectorType
  case object SealedTraitOrAbstractClass extends SelectorType

  type RefactoringParameters = Unit

  def prepare(s: Selection): Either[PreparationError, PreparationResult] = {
    val selectedTree = s.findSelectedWithPredicate {
      case _: SymTree | _: TermTree => true
      case _ => false
    }

    selectedTree match {
      case Some(Match(_, cases)) if cases.nonEmpty =>
        Left(PreparationError("Match block is not empty"))
      case Some(m @ Match(selector, _)) if selector.tpe.typeSymbol.isModuleClass =>
        Right(PreparationResult(m, SingleCaseObject))
      case Some(m @ Match(selector, _)) if selector.tpe.typeSymbol.isCaseClass =>
        Right(PreparationResult(m, SingleCaseClass))
      case Some(m @ Match(selector, _)) if (selector.tpe.typeSymbol.isTrait || selector.tpe.typeSymbol.isAbstractClass)
        && selector.tpe.typeSymbol.isSealed =>
        Right(PreparationResult(m, SealedTraitOrAbstractClass))
      case Some(m @ Match(selector, _)) =>
        Left(PreparationError(s"Selector is not a case class or sealed trait: ${showRaw(selector)}\n${showRaw(m)}"))
      case Some(otherwise) =>
        Left(PreparationError(s"Selection is not inside a match block: ${showRaw(otherwise)}"))
      case None =>
        Left(PreparationError("No selected SymTree/TermTree found"))
    }
  }

  def perform(selection: Selection, prepared: PreparationResult, params: Unit): Either[RefactoringError, List[Change]] = {
    val caseDefs = prepared.selectorType match {
      case SingleCaseClass | SingleCaseObject =>
        val tpe = prepared.matchBlock.selector.tpe
        val maybePat = toPatDef(tpe)

        maybePat.right.map(List(_))

      case SealedTraitOrAbstractClass =>
        val tpe = prepared.matchBlock.selector.tpe
        val ctors = collectCtors(tpe)
        val maybePats = ctors map toPatDef
        val errs = maybePats collect { case Left(e) => e }
        val pats = maybePats collect { case Right(p) => p }

        if (errs.nonEmpty) Left(RefactoringError(errs.map(_.cause).mkString("\n")))
        else Right(pats)
    }

    caseDefs.right.map { defs =>
      val replacement =
        topdown {
          matchingChildren {
            once {
              filter {
                case prepared.matchBlock => true
              } &>
                transform {
                  case m @ Match(selector, _) =>
                    Match(selector.duplicate.setPos(NoPosition), defs)
                }
            }
          }
        }

      transformFile(selection.file, replacement)
    }
  }

  def collectCtors(baseTpe: Type): List[Type] = {
    def ctors(sym: ClassSymbol): List[ClassSymbol] =
      sym.knownDirectSubclasses.toList.flatMap { subclass =>
        val subSym = subclass.asClass
        if (subSym.isCaseClass || sym.isModuleClass) List(subSym)
        else if ((subSym.isTrait || subSym.isAbstractClass) && subSym.isSealed) ctors(subSym)
        else List()
      }

    ctors(baseTpe.typeSymbol.asClass).map(_.tpe)
  }

  def toPatDef(tpe: Type): Either[RefactoringError, CaseDef] =
    if (tpe.typeSymbol.asClass.isModuleClass) Right(CaseDef(caseObjectPattern(tpe), Ident("???")))
    else if (tpe.typeSymbol.asClass.isCaseClass) Right(CaseDef(caseClassExtractionPattern(tpe), Ident("???")))
    else Left(RefactoringError(s"${tpe} is not a case object or case class in toPatDef"))

  def caseClassExtractionPattern(tpe: Type): Tree = {
    val fields: List[Tree] = tpe.decls.collect {
      case f: TermSymbol if f.isPublic && f.isCaseAccessor => Ident(f.name.toTermName)
    }.toList
    val companion = tpe.typeSymbol.companionSymbol

    Apply(companion, fields: _*)
  }

  def caseObjectPattern(tpe: Type): Tree = Ident(tpe.typeSymbol)
}
