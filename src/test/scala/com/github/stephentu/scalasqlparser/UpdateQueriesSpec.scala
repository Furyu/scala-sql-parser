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
    // Oracle specifics function `to_timestamp` is used
    val q3 =
      """
UPDATE
  `users`
SET
  `id1`=1000,
  `id2`='2000',
  `nickname`='mumoshu',
  `updated_time`=to_timestamp('2012-12-14 12:45:39.000','yyyy-MM-dd hh24:mi:ss.ff3')
WHERE
  ((`users`.`id1`=1000) and (`users`.`id2`=2000))
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
    "parse query3" in {
      val parser = new SQLParser
      val r = parser.parse(Queries.q3)
      r should beSome
    }
  }

}
