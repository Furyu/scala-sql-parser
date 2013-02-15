name := "scala-sql-parser"

version := "0.1.3"

organization := "jp.furyu.scalasqlparser"

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.2",
  "postgresql" % "postgresql" % "9.1-901-1.jdbc4" % "test",
  "org.specs2" %% "specs2" % "1.12.3" % "test",
  "mysql" % "mysql-connector-java" % "5.1.21" % "test",
  "ch.qos.logback" % "logback-classic" % "1.0.9" % "test",
  "ch.qos.logback" % "logback-core" % "1.0.9" % "test"
)

scalacOptions += "-deprecation"

scalacOptions += "-unchecked"
