// Copyright: 2010 - 2017 https://github.com/ensime/ensime-server/graphs
// License: http://www.gnu.org/licenses/gpl-3.0.en.html
package org.ensime.core

import scala.tools.nsc.interactive.Global

/**
 * Type safety around Scala type names, scalac uses raw Strings.
 */
final class ScalaName(val underlying: String) extends AnyVal {
  def +(other: String): ScalaName =
    if (other == "") this
    else new ScalaName(underlying + other)
}

trait TypeToScalaName { self: Global with Helpers =>
  import definitions._

  def scalaName(tpe: Type, full: Boolean, shouldDealias: Boolean = true): ScalaName = {
    val typeSymbol = if (shouldDealias) tpe.typeSymbol else tpe.typeSymbolDirect

    tpe match {
      case _: MethodType | _: PolyType =>
        val tparams = tpe.paramss.map { sect =>
          sect.map { p => scalaName(p.tpe, full).underlying }.mkString("(", ", ", ")")
        }.mkString(" => ")
        new ScalaName(tparams) + " => " + scalaName(tpe.finalResultType, full).underlying

      case args: ArgsTypeRef if typeSymbol.fullName.startsWith("scala.Function") =>
        val parts = tpe.typeArgs.map(scalaName(_, full).underlying)
        new ScalaName(parts.init.mkString("(", ", ", ")")) + " => " + parts.last

      case args: ArgsTypeRef if typeSymbol.fullName.startsWith("scala.Tuple") =>
        val parts = tpe.typeArgs.map(scalaName(_, full).underlying)
        new ScalaName(parts.mkString("(", ", ", ")"))

      case args: ArgsTypeRef if typeSymbol.decodedName.forall(!_.isLetterOrDigit) =>
        val parts = tpe.typeArgs.map(scalaName(_, full).underlying)
        val name =
          if (full) tpe.typeSymbol.fullNameString
          else tpe.typeSymbol.nameString

        new ScalaName(parts.init.mkString(" ")) + s" $name ${parts.last}"

      case TypeRef(_, RepeatedParamClass | JavaRepeatedParamClass, typeArgs) =>
        // Safe to assume that args is of length 1 as repeated params means Seq[args]
        val parts = typeArgs.map(scalaName(_, full).underlying)
        new ScalaName(parts.mkString + "*")

      case TypeRef(_, ByNameParamClass, typeArgs) =>
        val parts = typeArgs.map(scalaName(_, full).underlying)
        new ScalaName(s"=> ${parts.mkString}")

      case _ =>
        val name = tpe match {
          case c: ConstantType =>
            scalaName(c.underlying, full).underlying + "(" + c.value.escapedStringValue + ")"
          case r: RefinedType =>
            r.parents.map(scalaName(_, full).underlying).mkString(" with ")

          case a: AliasTypeRef if !shouldDealias =>
            if (full) tpe.typeSymbolDirect.fullNameString
            else tpe.typeSymbolDirect.nameString

          case _ =>
            if (full) tpe.typeSymbol.fullNameString
            else tpe.typeSymbol.nameString
        }

        new ScalaName(name) + {
          val typeArgs = if (shouldDealias) tpe.dealias.typeArgs else tpe.typeArgs

          if (typeArgs.isEmpty) ""
          else typeArgs.map(scalaName(_, full).underlying).mkString("[", ", ", "]")
        }
    }
  }

  def fullName(tpe: Type, shouldDealias: Boolean = true): ScalaName = scalaName(tpe, full = true, shouldDealias = shouldDealias)
  def shortName(tpe: Type, shouldDealias: Boolean = true): ScalaName = scalaName(tpe, full = false, shouldDealias = shouldDealias)

  def shortName(sym: Symbol): ScalaName = new ScalaName(sym.nameString)

}
