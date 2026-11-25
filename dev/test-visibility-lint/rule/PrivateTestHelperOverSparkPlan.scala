package fix

import scala.annotation.tailrec
import scala.meta._
import scalafix.v1._

/**
 * SPARK-54301-style lint: finds `private`/nested helper members inside Spark test
 * "Suite" base classes whose body pattern-matches (or type-ascribes / isInstanceOf's)
 * on a type that is, or extends, SparkPlan or Expression. Such members cannot be
 * overridden by subclasses (e.g. Gluten's GlutenXxxSuite extends XxxSuiteBase), which
 * forces full copy-paste of the test instead of overriding just the plan-shape check.
 *
 * Classifies each hit as:
 *   A - top-level `private def`/`private val` member of the Suite class -> widen to `protected`
 *   B - a `def` nested inside the body of another method -> hoist to a class-level `protected def`
 *   C - (informational) the matched type is a concrete exec class with a plausibly better
 *       existing abstract sibling trait (seed table below) that Gluten (or similar) could match
 *       against instead, following the ShuffleExchangeExec -> ShuffleExchangeLike precedent.
 */
class PrivateTestHelperOverSparkPlan extends SemanticRule("PrivateTestHelperOverSparkPlan") {

  // Fully-qualified semanticdb symbols of the two "interesting" base types.
  private val targetBaseSymbols: Set[String] = Set(
    "org/apache/spark/sql/execution/SparkPlan#",
    "org/apache/spark/sql/catalyst/expressions/Expression#"
  )

  // Seed table: concrete exec class symbol -> known existing abstract sibling (Fix-C hint).
  // Deliberately small & curated; NOT meant to be exhaustive or mechanically derived.
  private val knownAbstractSiblings: Map[String, String] = Map(
    "org/apache/spark/sql/execution/exchange/ShuffleExchangeExec#" ->
      "org.apache.spark.sql.execution.exchange.ShuffleExchangeLike",
    "org/apache/spark/sql/execution/FileSourceScanExec#" ->
      "org.apache.spark.sql.execution.FileSourceScanLike"
  )

  private def isSuiteLikeName(name: String): Boolean =
    name.contains("Suite") || name.endsWith("TestBase")

  @tailrec
  private def findEnclosingSuite(t: Tree): Option[(String, Tree)] = t.parent match {
    case Some(d @ Defn.Class(_, name, _, _, _)) if isSuiteLikeName(name.value) =>
      Some((name.value, d))
    case Some(d @ Defn.Trait(_, name, _, _, _)) if isSuiteLikeName(name.value) =>
      Some((name.value, d))
    case Some(parent) => findEnclosingSuite(parent)
    case None => None
  }

  // Is `member` nested lexically inside another Defn.Def's body (rather than being a
  // direct statement of the class/trait template)?
  private def nestedInsideAnotherDef(member: Tree): Boolean = {
    @tailrec def loop(t: Tree): Boolean = t.parent match {
      case Some(_: Template) => false
      case Some(_: Defn.Def) => true
      case Some(p) => loop(p)
      case None => false
    }
    loop(member)
  }

  // Walk the supertype chain of `symbol` (as recorded in its ClassSignature's parents),
  // returning the target base symbol first matched, if any.
  private def supertypeChainHitsTarget(
      symbol: Symbol,
      seen: Set[String] = Set.empty
  )(implicit doc: SemanticDocument): Option[String] = {
    val symValue = symbol.value
    if (seen(symValue)) None
    else if (targetBaseSymbols.contains(symValue)) Some(symValue)
    else
      symbol.info.flatMap { info =>
        info.signature match {
          case cs: ClassSignature =>
            val parentSymbols = cs.parents.collect { case TypeRef(_, psym, _) => psym }
            parentSymbols.view
              .flatMap(ps => supertypeChainHitsTarget(ps, seen + symValue))
              .headOption
          case _ => None
        }
      }
  }

  // Collect all "type mentions" inside a member's body that could plausibly be an
  // operator-exec pattern match / type-ascription: `case x: T =>`, `x: T` in Pat.Typed,
  // and `.isInstanceOf[T]` / `.asInstanceOf[T]`.
  private def typeMentionsIn(body: Tree): List[Type] = {
    val buf = List.newBuilder[Type]
    body.traverse {
      case Pat.Typed(_, tpe) => buf += tpe
      case Case(Pat.Typed(_, tpe), _, _) => buf += tpe
      case Case(Pat.Bind(_, Pat.Typed(_, tpe)), _, _) => buf += tpe
      case Term.ApplyType(Term.Select(_, Term.Name("isInstanceOf")), List(tpe)) => buf += tpe
      case Term.ApplyType(Term.Select(_, Term.Name("asInstanceOf")), List(tpe)) => buf += tpe
    }
    buf.result()
  }

  private def typeSymbol(tpe: Type)(implicit doc: SemanticDocument): Option[Symbol] =
    tpe match {
      case t: Type.Name => Some(t.symbol)
      case t: Type.Select => Some(t.symbol)
      case t: Type.Apply => typeSymbol(t.tpe)
      case other => Some(other.symbol)
    }

  override def fix(implicit doc: SemanticDocument): Patch = {
    val hits = List.newBuilder[Patch]

    def handleMember(memberTree: Tree, memberName: String, body: Tree, isPrivateTopLevel: Boolean): Unit = {
      findEnclosingSuite(memberTree).foreach { case (suiteName, _) =>
        val nested = nestedInsideAnotherDef(memberTree)
        // Only report when the member is either a top-level `private` member of the
        // suite, or a nested def (both are invisible to subclasses).
        if (isPrivateTopLevel || nested) {
          val mentions = typeMentionsIn(body)
          mentions.foreach { tpe =>
            typeSymbol(tpe).foreach { sym =>
              if (sym != Symbol.None) {
                supertypeChainHitsTarget(sym) match {
                  case Some(targetSym) =>
                    val fixCategory = if (nested) "B" else "A"
                    val siblingHint = knownAbstractSiblings.get(sym.value) match {
                      case Some(sibling) =>
                        s" [Fix-C candidate: $sym has known abstract sibling $sibling]"
                      case None => ""
                    }
                    val pos = memberTree.pos
                    val msg =
                      s"[$fixCategory] ${if (nested) "nested def" else "private member"} " +
                        s"'$memberName' in '$suiteName' pattern-matches/ascribes type " +
                        s"'${sym.displayName}' (-> $targetSym in supertype chain)$siblingHint. " +
                        s"Subclasses cannot override this member to swap in a different " +
                        s"physical-operator type."
                    hits += Patch.lint(Diagnostic("privateTestHelperOverSparkPlan", msg, pos))
                  case None => ()
                }
              }
            }
          }
        }
      }
    }

    doc.tree.traverse {
      case d @ Defn.Def(mods, name, _, _, _, body) =>
        val isPrivate = mods.exists(_.is[Mod.Private])
        handleMember(d, name.value, body, isPrivateTopLevel = isPrivate)
      case d @ Defn.Val(mods, pats, _, rhs) =>
        val isPrivate = mods.exists(_.is[Mod.Private])
        val name = pats.headOption.map(_.syntax).getOrElse("<val>")
        handleMember(d, name, rhs, isPrivateTopLevel = isPrivate)
      case d @ Defn.Var(mods, pats, _, rhsOpt) =>
        val isPrivate = mods.exists(_.is[Mod.Private])
        val name = pats.headOption.map(_.syntax).getOrElse("<var>")
        rhsOpt.foreach(rhs => handleMember(d, name, rhs, isPrivateTopLevel = isPrivate))
    }

    hits.result().asPatch
  }
}
