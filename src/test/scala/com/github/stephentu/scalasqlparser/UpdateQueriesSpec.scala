package com.github.stephentu.scalasqlparser

import org.specs2.mutable._

object UpdateQueriesSpec extends Specification {

  object Queries {
    val q1 = """
UPDATE `posts` SET `title`='one-modified' WHERE (`posts`.`id`=1)
    """
    val q2 =
      """
UPDATE
  `posts` `p`
JOIN
  `users` `u`
ON
  p.user_id = u.id SET p.`title`='new_title'
WHERE
  u.`user_id` = 1
      """.stripMargin
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
