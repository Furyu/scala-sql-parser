package com.github.stephentu.scalasqlparser

import org.specs2.mutable._

object InsertQueriesSpec extends Specification {

  object Queries {
    val q1 = """
insert into posts SET id = 1, title = "first post"
             """
    val q2 = """
INSERT INTO
  posts
VALUES
  (0 + 1, "first post")
"""
  }

  "SQLParser" should {
    "parse query1" in {
      val parser = new SQLParser
      val r = parser.parse(Queries.q1)
      r should beSome
    }
    "parse query2" in {
      val parser = new SQLParser
      val r = parser.parse(Queries.q2)
      r should beSome
    }
  }

}
