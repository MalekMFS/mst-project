package mstproject

import org.apache.spark.sql.SparkSession

object SparkConstructor {
  def apply(): SparkSession = {
    val ss = SparkSession.builder
      .appName("mst-project")
      .master("local[*]")
      //    .master("spark://127.0.0.1:7077") //for production
//      .config("spark.driver.memory", "8g")
//      .config("spark.driver.memoryOverhead", "1g")
//      .config("spark.executor.memory", "8g")
//      .config("spark.executor.memoryOverhead", "1g")
      .config("spark.redis.host", "localhost") // for redisSpark
      .config("spark.redis.port", "6379")
      .getOrCreate()
    ss.sparkContext.setLogLevel("WARN")
    ss
  }
}
