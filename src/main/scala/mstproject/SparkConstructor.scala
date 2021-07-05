package mstproject

import org.apache.spark.sql.SparkSession

object SparkConstructor {
  def apply() = {
    val ss = SparkSession.builder
    .appName("mst-project")
    .master("local[*]") // Change this to cluster mode in production
    .config("spark.redis.host", "localhost") // for redisSpark
    .config("spark.redis.port", "6379")
    .getOrCreate()
    ss.sparkContext.setLogLevel("WARN")
    ss
  }
}
