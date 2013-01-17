package com.github.stephentu.scalasqlparser

import org.specs2.mutable._

object InsertQueriesSpec extends Specification {

  object InsertQueries {
    val q1 = """
insert into posts SET id = 1, title = "first post"
             """
    val q2 = """
INSERT INTO
  posts
VALUES
  (0 + 1, "first post")
"""
    val q3 =
      """
INSERT INTO
 `posts`
 (`id`,`title`)
VALUES
  (1,'first post');
      """.stripMargin
  }

  "SQLParser" should {
    "parse query1" in {
      val parser = new SQLParser
      val r = parser.parse(InsertQueries.q1)
      r should beSome
    }
    "parse query2" in {
      val parser = new SQLParser
      val r = parser.parse(InsertQueries.q2)
      r should beSome
    }
    "parse query3" in {
      val parser = new SQLParser
      val r = parser.parse(InsertQueries.q3)
      r should beSome
    }
    "parse query23" in {
      val parser = new SQLParser
      val r = parser.parse(Queries.q23)
      r should beSome
    }
  }

}
