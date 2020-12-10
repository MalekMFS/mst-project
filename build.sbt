name := "mst-project"

version := "0.1"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "com.github.pathikrit" %% "better-files" % "3.9.1",
  "org.scalatest" %% "scalatest" % "3.2.3" % Test,
  "org.apache.spark" %% "spark-core" % "3.0.1",
  "org.apache.spark" %% "spark-sql" % "3.0.1"
)