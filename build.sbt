name := "scala-sql-parser"

version := "0.1-SNAPSHOT"

organization := "com.github.stephentu.scalasqlparser"

libraryDependencies ++= Seq(
  "postgresql" % "postgresql" % "9.1-901-1.jdbc4" % "test",
  "org.specs2" %% "specs2" % "1.12.3" % "test"
)

scalacOptions += "-deprecation"

scalacOptions += "-unchecked"
