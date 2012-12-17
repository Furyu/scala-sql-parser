package com.github.stephentu.scalasqlparser

import java.sql._
import java.util.Properties

// these are the types of relations which can show up in a
// FROM clause
abstract trait Relation
case class TableRelation(name: String) extends Relation
case class SubqueryRelation(stmt: SelectStmt) extends Relation

case class TableColumn(name: String, tpe: DataType) extends PrettyPrinters {
  def scalaStr: String =
    "TableColumn(" + _q(name) + ", " + tpe.toString + ")"
}

class Definitions(val defns : Map[String, Seq[TableColumn]]) extends PrettyPrinters {

  def tableExists(table: String): Boolean = defns.contains(table)

  def lookup(table: String, col: String): Option[TableColumn] = {
    defns.get(table).flatMap(_.filter(_.name == col).headOption)
  }

  def scalaStr: String = {
    "new Definitions(Map(" + (defns.map { case (k, v) =>
      (_q(k), "Seq(" + v.map(_.scalaStr).mkString(", ") + ")")
    }.map { case (k, v) => k + " -> " + v }.mkString(", ")) + "))"
  }
}

trait Schema {
  def loadSchema(): Definitions
}

class PgSchema(hostname: String, port: Int, db: String, props: Properties) extends Schema {
  Class.forName("org.postgresql.Driver")
  private val conn = DriverManager.getConnection(
    "jdbc:postgresql://%s:%d/%s".format(hostname, port, db), props)

  def loadSchema() = {
    import Conversions._
    val s = conn.prepareStatement("""
select table_name from information_schema.tables
where table_catalog = ? and table_schema = 'public'
      """)
    s.setString(1, db)
    val r = s.executeQuery
    val tables = r.map(_.getString(1))
    s.close()

    new Definitions(tables.map(name => {
      val s = conn.prepareStatement("""
select
  column_name, data_type, character_maximum_length,
  numeric_precision, numeric_precision_radix, numeric_scale
from information_schema.columns
where table_schema = 'public' and table_name = ?
""")
      s.setString(1, name)
      val r = s.executeQuery
      val columns = r.map(rs => {
        val cname = rs.getString(1)
        TableColumn(cname, rs.getString(2) match {
          case "character varying" => VariableLenString(rs.getInt(3))
          case "character" => FixedLenString(rs.getInt(3))
          case "date" => DateType
          case "numeric" => DecimalType(rs.getInt(4), rs.getInt(6))
          case "integer" =>
            assert(rs.getInt(4) % 8 == 0)
            IntType(rs.getInt(4) / 8)
          case "bigint" => IntType(8)
          case "smallint" => IntType(2)
          case "bytea" => VariableLenByteArray(None)
          case e => sys.error("unknown type: " + e)
        })
      })
      s.close()
      (name, columns)
    }).toMap)
  }
}

object MySQLSchema {
  // Register the driver for the case that MySQLSchema is used independently
  // (e.g. other part of your application does not use MySQL driver)
  Class.forName("com.mysql.jdbc.Driver")

  def apply(hostname: String, port: Int, db: String, props: Properties): MySQLSchema = {
    val conn = DriverManager.getConnection(
      "jdbc:mysql://%s:%d/%s".format(hostname, port, db), props)
//    val driver = Class.forName("com.mysql.jdbc.Driver").getConstructor().newInstance().asInstanceOf[java.sql.Driver]
//    val conn = driver.connect("jdbc:mysql://%s:%d/%s".format(hostname, port, db), props)
    new MySQLSchema(conn)
  }
}

/**
 * The schema provider for MySQL
 * @param conn the connection where the database schema is look for.
 */
class MySQLSchema(conn: Connection) extends Schema {

  def loadSchema() = {
    import Conversions._
    val db = conn.getSchema
    val s = conn.prepareStatement("""
select table_name from information_schema.tables
where table_schema = ?
      """)
    s.setString(1, db)
    val r = s.executeQuery
    val tables = r.map(_.getString(1))
    s.close()

    new Definitions(tables.map(name => {
      val s = conn.prepareStatement("""
select
  column_name, data_type, character_maximum_length, character_octet_length,
  numeric_precision, numeric_scale
from information_schema.columns
where table_schema = ? and table_name = ?
        """)
      s.setString(1, db)
      s.setString(2, name)
      val r = s.executeQuery
      val columns = r.map(rs => {
        val cname = rs.getString(1)
        TableColumn(cname, rs.getString(2) match {
          case "tinytext" | "text" | "mediumtext" | "longtext" =>
            VariableLenString(rs.getInt(3))
          case "char" | "varchar" =>
            FixedLenString(rs.getInt(3))
          case "tinyint" =>
            IntType(1)
          case "smallint" =>
            IntType(2)
          case "mediumint" =>
            IntType(3)
          case "int" =>
            IntType(4)
          case "bigint" =>
            IntType(8)
          case "float" | "double" =>
            UnknownType
          case "date" | "datetime" | "timestamp" =>
            DateType
          case "numeric" | "decimal" =>
            DecimalType(rs.getInt(6), rs.getInt(5))
          case "tinyblob" | "blob" | "mediumblob" | "longblob" =>
            VariableLenByteArray(Some(rs.getInt(4)))
          case e =>
            sys.error("unknown type: " + e)
        })
      })
      s.close()
      (name, columns)
    }).toMap)
  }
}
