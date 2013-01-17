package com.github.stephentu.scalasqlparser

import org.specs2.mutable._

class ResolverSpec extends Specification {

  object resolver extends Resolver

  private def doTest(q: String) = {
    val parser = new SQLParser
    val r = parser.parse(q)
    resolver.resolve(r.get, TestSchema.definition)
  }

  "Resolver" should {
    "resolve query1" in {
      val s0 = doTest(Queries.q1)
      s0.ctx.projections.size must_== 10
    }

    "resolve query2" in {
      val s0 = doTest(Queries.q2)
      s0.ctx.projections.size must_== 8
    }

    "resolve query3" in {
      val s0 = doTest(Queries.q3)
      s0.ctx.projections.size must_== 4
    }

    "resolve query4" in {
      val s0 = doTest(Queries.q4)
      s0.ctx.projections.size must_== 2
    }

    "resolve query5" in {
      val s0 = doTest(Queries.q5)
      s0.ctx.projections.size must_== 2
    }

    "resolve query6" in {
      val s0 = doTest(Queries.q6)
      s0.ctx.projections.size must_== 1
    }

    "resolve query7" in {
      val s0 = doTest(Queries.q7)
      s0.ctx.projections.size must_== 4
    }

    "resolve query8" in {
      val s0 = doTest(Queries.q8)
      s0.ctx.projections.size must_== 2
    }

    "resolve query9" in {
      val s0 = doTest(Queries.q9)
      s0.ctx.projections.size must_== 3
    }

    "resolve query10" in {
      val s0 = doTest(Queries.q10)
      s0.ctx.projections.size must_== 8
    }

    "resolve query11" in {
      val s0 = doTest(Queries.q11)
      s0.ctx.projections.size must_== 2
    }

    "resolve query12" in {
      val s0 = doTest(Queries.q12)
      s0.ctx.projections.size must_== 3
    }

    "resolve query13" in {
      val s0 = doTest(Queries.q13)
      s0.ctx.projections.size must_== 2
    }

    "resolve query14" in {
      val s0 = doTest(Queries.q14)
      s0.ctx.projections.size must_== 1
    }

    "resolve query16" in {
      val s0 = doTest(Queries.q16)
      s0.ctx.projections.size must_== 4
    }

    "resolve query17" in {
      val s0 = doTest(Queries.q17)
      s0.ctx.projections.size must_== 1
    }

    "resolve query18" in {
      val s0 = doTest(Queries.q18)
      s0.ctx.projections.size must_== 6
    }

    "resolve query19" in {
      val s0 = doTest(Queries.q19)
      s0.ctx.projections.size must_== 1
    }

    "resolve query20" in {
      val s0 = doTest(Queries.q20)
      s0.ctx.projections.size must_== 2
    }

    "resolve query21" in {
      val s0 = doTest(Queries.q21)
      s0.ctx.projections.size must_== 2
    }

    "resolve query22" in {
      val s0 = doTest(Queries.q22)
      s0.ctx.projections.size must_== 3
    }

    "resolve query23" in {
      val s0 = doTest(Queries.q23)
      val stmt = s0.asInstanceOf[InsertStmt]
      val values = stmt.insRow.asInstanceOf[Values].values
      stmt.table.name must be equalTo ("customer")
      values(0).symbol must be equalTo (Some(ColumnSymbol("customer", "c_comment", values(0).ctx)))
      values(1).symbol must be equalTo (Some(ColumnSymbol("customer", "c_mktsegment", values(1).ctx)))
      values(2).symbol must be equalTo (Some(ColumnSymbol("customer", "c_acctbal", values(2).ctx)))
      values(3).symbol must be equalTo (Some(ColumnSymbol("customer", "c_phone", values(3).ctx)))
      values(4).symbol must be equalTo (Some(ColumnSymbol("customer", "c_nationkey", values(4).ctx)))
      values(5).symbol must be equalTo (Some(ColumnSymbol("customer", "c_address", values(5).ctx)))
      values(6).symbol must be equalTo (Some(ColumnSymbol("customer", "c_name", values(6).ctx)))
      values(7).symbol must be equalTo (Some(ColumnSymbol("customer", "c_custkey", values(7).ctx)))
    }

    "resolve query24" in {
      val s0 = doTest(Queries.q24)
      val stmt = s0.asInstanceOf[InsertStmt]
      val values = stmt.insRow.asInstanceOf[NamedValues].values
      stmt.table.name must be equalTo ("customer")
      values(7).field.symbol must be equalTo (Some(ColumnSymbol("customer", "c_comment", values(7).ctx)))
      values(6).field.symbol must be equalTo (Some(ColumnSymbol("customer", "c_mktsegment", values(6).ctx)))
      values(5).field.symbol must be equalTo (Some(ColumnSymbol("customer", "c_acctbal", values(5).ctx)))
      values(4).field.symbol must be equalTo (Some(ColumnSymbol("customer", "c_phone", values(4).ctx)))
      values(3).field.symbol must be equalTo (Some(ColumnSymbol("customer", "c_nationkey", values(3).ctx)))
      values(2).field.symbol must be equalTo (Some(ColumnSymbol("customer", "c_address", values(2).ctx)))
      values(1).field.symbol must be equalTo (Some(ColumnSymbol("customer", "c_name", values(1).ctx)))
      values(0).field.symbol must be equalTo (Some(ColumnSymbol("customer", "c_custkey", values(0).ctx)))
    }
  }
}
