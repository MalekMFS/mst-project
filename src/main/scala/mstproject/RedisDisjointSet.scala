package mstproject

import com.redislabs.provider.redis.toRedisContext
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

object RedisDisjointSet{
  //    val redisConfig = RedisConfig.fromSparkConf(spark.spark)
  @transient lazy val log: Logger = org.apache.log4j.LogManager.getLogger("myLogger")
  def apply(set: RDD[Int]): Unit = {
    val spark = SparkConstructor()
    val sc = spark.sparkContext
    // initialize Parents and Ranks key-values
    val parents = set.map(i => ("p"+i, i.toString))
    val ranks = set.map(i => ("r"+i, 1.toString))
    sc.toRedisKV(parents)
    sc.toRedisKV(ranks)
    log.warn("***Initialized Redis's Disjointset***")

  }
  val jedisConfig = new JedisPoolConfig()
  jedisConfig.setMaxIdle(100) //TODO: a better configuration?
  jedisConfig.setMaxTotal(200)
  val pool = new JedisPool(jedisConfig, "localhost")
  // Unions sets and returns status code
  def union(u: Long, v: Long): Int = {
    // status == 0: already in the same set
    // status == 1: update u's parent to v
    // status == 2 or 3: update v's parent to u
    val r = pool.getResource
//    log.warn("***Inside Union func***")
    var statusCode = 0
    for {
      (x, y) <- parents(u, v)
      (xr, yr) <- ranks(u, v)
    } yield {
      if (x != y) (xr, yr) match {
        case _ if xr < yr => r.mset("p" + x, y.toString); r.decr("components"); statusCode = 1
        case _ if xr > yr => r.mset("p" + y, x.toString); r.decr("components"); statusCode = 2
        case _ => r.mset("p" + y, x.toString); r.mset("r" + x, (xr + 1).toString); r.decr("components"); statusCode = 3
      }
    }
    r.close()
    statusCode
  }
  def find(u: Long): Option[Long] = { // returns leader of the set containing u
    val r = pool.getResource
    val res = Option(r.get(s"p$u")).flatMap(p => if (p.toLong == u) {
//      log.warn(s"*** Inside find func. u = $u , p = $p ***")
      Some(u)
    } else find(p.toLong))
    r.close()
    res
  }
  def componentsCount: Long = {
    val r = pool.getResource
    val res = r.get("components").toLong
    r.close()
    res
  }
  def iterationCount: Int = {
    val r = pool.getResource
    val res = r.get("iteration").toInt
    r.close()
    res
  }
  def iterationInc: Long = {
    val r = pool.getResource
    val it = r.incr("iteration")
    r.close()
    it
  }
  private def parents(u: Long, v: Long): Option[(Long, Long)] = for {x <- find(u); y <- find(v)} yield (x,y) // return parents for u and v
  private def ranks(u: Long, v: Long): Option[(Long, Long)] = {
    val r = pool.getResource
    val res = for {x <- Option(r.get("r" + u)); y <- Option(r.get("r" + v))} yield (x.toLong, y.toLong)
    r.close()
    res
  }
}
