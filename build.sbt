name := "mst-project"

version := "0.1"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "com.github.pathikrit" %% "better-files" % "3.9.1",
  "org.scalatest" %% "scalatest" % "3.2.3" % Test,
  "org.apache.spark" %% "spark-core" % "2.4.7",
  "org.apache.spark" %% "spark-sql" % "2.4.7",
  "org.apache.spark" %% "spark-graphx" % "2.4.7",
  "redis.clients" % "jedis" % "3.5.1", // https://mvnrepository.com/artifact/redis.clients/jedis
  "com.redislabs" %% "spark-redis" % "2.4.2"
)