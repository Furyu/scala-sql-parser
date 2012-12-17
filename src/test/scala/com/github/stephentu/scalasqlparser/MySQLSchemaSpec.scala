package com.github.stephentu.scalasqlparser

import org.specs2.mutable._
import java.sql.DriverManager

object MySQLSchemaSpec extends Specification {

  "MySQLSchema" should {
    "provide a schema from an existing connection" in {
      Class.forName("com.mysql.jdbc.Driver")
      val conn = DriverManager.getConnection("jdbc:mysql://localhost/test")
      new MySQLSchema(conn).loadSchema()
      conn.close()
      success
    }
    "provide a schema from host, port and database name" in {
      Class.forName("com.mysql.jdbc.Driver")
      val schema = MySQLSchema("localhost", 3306, "test", new java.util.Properties())
      success
    }
  }

}
