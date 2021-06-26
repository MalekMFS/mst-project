package mstproject

import mstproject.Model._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import com.redislabs.provider.redis._
import org.apache.log4j.{Level, LogManager}
import redis.clients.jedis.Jedis

import scala.annotation.tailrec

object SparkConstructor {
  def apply() = SparkSession.builder
        .appName("Boruvka")
        .master("local[1]") // Change this to cluster mode in production
        .config("spark.redis.host", "localhost")
        .config("spark.redis.port", "6379")
        .getOrCreate()
}

object redisDisjointSet{
//    val redisConfig = RedisConfig.fromSparkConf(spark.spark)
@transient lazy val log = org.apache.log4j.LogManager.getLogger("myLogger")
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
  lazy val r = new Jedis("127.0.0.1",6379,30) ///////////////////////
  // Unions sets and returns status code
  def union(u: Long, v: Long): Int = {
    // status == 0: already in the same set
    // status == 1: update u's parent to v
    // status == 2 or 3: update v's parent to u
    log.warn("***Inside Union func***")
    var statusCode = 0
    for {
      (x, y) <- parents(u, v)
      (xr, yr) <- ranks(u, v)
    } yield {
      if (x != y) (xr, yr) match {
        case _ if xr < yr => r.mset("p" + x, y.toString);  statusCode = 1 //;r.decr("components")
        case _ if xr > yr => r.mset("p" + y, x.toString); statusCode = 2 //;r.decr("components")
        case _ => r.mset("p" + y, x.toString); r.mset("r" + x, (xr + 1).toString); statusCode = 3 //;r.decr("components")
      }
    }
    return statusCode
  }
  def find(u: Long): Option[Long] = {
    Option(r.get(s"p$u")).flatMap(p => if (p.toLong == u) {
      log.warn(s"*** Inside find func. u = $u , p = $p ***")
      Some(u)
    } else find(p.toLong))

  } // returns leader of the set containing u
  def componentsCount: Long = r.get("components").toLong
  private def parents(u: Long, v: Long): Option[(Long, Long)] = for {x <- find(u); y <- find(v)} yield (x,y) // return parents for u and v
  private def ranks(u: Long, v: Long): Option[(Long, Long)] = for {x <- Option(r.get("r"+u)); y <- Option(r.get("r"+v))} yield (x.toLong,y.toLong)
  private def closeConnection(): Unit = r.close()
}

object BoruvkaRDD_RS {
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("myLogger")
  /** Returns updated parents */
  def addEdges(parents: RDD[(Int, Int)], selectedEdges: RDD[weightedEdge]): RDD[(Int, Int)] = {
    val affectingEdges: RDD[(weightedEdge, Int)] = selectedEdges.map( e => (e, redisDisjointSet.union(e.edge.u, e.edge.v))).filter(_._2 != 0) // ignore inner edge
//    println(affectingEdges.first().toString())
    val ones = affectingEdges.filter(_._2 == 1).map(_._1)
    val twoThrees = affectingEdges.filter(_._2 != 1).map(_._1)
    val updatedParents: RDD[(Int, Int)] = parents.mapPartitions{ part => ///ERROR: don't use another rdd inside a rdd transformation
      part.map { parent =>
        val oneEdge = ones.filter(_.edge.u == parent._1)
        if(!oneEdge.isEmpty)
          (parent._1, oneEdge.first.edge.v)
        else{
          val twoThreeEdge = twoThrees.filter(_.edge.v == parent._1)
          if (!twoThreeEdge.isEmpty)
            (parent._1, twoThreeEdge.first.edge.u)
          else
            parent
        }
      }
    }
    updatedParents
//    val e = selectedEdges.head
//    val status: Int = redisDisjointSet.union(e.edge.u, e.edge.v) // union Disjoint sets
//    val updatedParents: RDD[(Int, Int)] = status match {
//      case 0 => parents // already in the same set
//      case 1 => parents.mapPartitions( partition => partition.map(row => if (row._1 == e.edge.u) (row._1, e.edge.v) else (row._1, row._2)) )// update u's parent to v
//      case 2 | 3 => parents.mapPartitions( partition => partition.map(row => if (row._1 == e.edge.v) (row._1, e.edge.u) else (row._1, row._2)) )// update v's parent to u
//    }
//    if (selectedEdges.tail.length > 0 )
//      addEdges(updatedParents, selectedEdges.tail)
//    else
//      updatedParents
  }

  @tailrec
  def BoruvkaLoop(parents: RDD[(Int, Int)], edges: RDD[weightedEdge], forest: RDD[weightedEdge]): List[weightedEdge] =
    {
      log.warn("*** Boruvka Loop ***")
      // set of connected Vertices (same as DisjointSet)
      val sets: RDD[(Option[Long], Iterable[Int])] = parents
        .mapPartitions ( _.map( x => (x._1, redisDisjointSet.find(x._2.toLong)) ))
        .groupBy(_._2) // group by parents
        .mapValues( _.map(_._1) )

      // TODO calculate and pass 'sets' at the end instead of passing 'parents'
      log.warn("*** After find for making sets ***")
      val components = sets.count
      log.warn(s"# components: $components")
      if (components > 1) { // for connected graphs //redisDisjointSet.componentsCount > 1
        /** foreach connected component select their minimum edge */
        val selectedEdges: RDD[weightedEdge] = sets.mapPartitions { part =>
          part.map {set =>
            val verticesInSet: Iterable[Int] = set._2
            // 1. Find cheapest outgoing edge (for each set)
            // 2. select the cheapest among them
            verticesInSet.map( v =>
              edges
                .filter(e => !e.removed) // ignore selected edges
                .filter(e => e.edge.u == v || e.edge.v == v) // edges connected to v
                .filter(e =>  redisDisjointSet.find(e.edge.u) != redisDisjointSet.find(e.edge.v) ) // ignore internal edges
                .min()(ord) // min edge for each vertex in a set
            ).minBy(_.weight) // min edge in each set
          }
        }
        // TODO partition sets by set id.

        /** Remove selected edges from the edgesMap for the next iterations */
        // val selecetedEdgesLocal = selectedEdges.collect()  // BAD SMELL!
        // idea1: left outer join
        // idea2: embed set to weighted edge? one RDD[weightedEdge]
        // NOTE: remember to use flatMap on iterators
  //      val newEdgeMap = edgesMap.leftOuterJoin(r => r._2 selectedEdges)
  //      .mapPartitions( it =>
  //        it.map( vToEdges =>
  //          (vToEdges._1, vToEdges._2.filterNot(e => selecetedEdgesLocal.contains(e)))
  //        )
  //      )
        val updatedEdges: RDD[weightedEdge] = edges.mapPartitions(part =>
          part.map(e => if (selectedEdges.filter(_ == e).isEmpty()) e else weightedEdge(e.edge, e.weight, removed = true)) // FIXME: Does comparison works?
        )
        /** union sets in this iteration */
        val newParents: RDD[(Int, Int)] = addEdges(parents, selectedEdges)

        BoruvkaLoop(newParents ,updatedEdges, forest ++ selectedEdges) // next iteration after updates
      }
      else forest.collect().toList // return the resulting forest
    }

  def apply(path: String, delim: String): List[weightedEdge] = {
    //TODO anywhere needed to cache data?

    lazy val spark: SparkSession = SparkConstructor()
    val E = spark.read
      .format("csv").option("delimiter",delim)
      .load(path).toDF("u", "v")
      .rdd
      .zipWithIndex
      .map( r => weightedEdge( myEdge(r._1.getAs[String](0).toInt, r._1.getAs[String](1).toInt), r._2.toInt) )
      .cache
    // val sc = spark.sparkContext

    /** Make all vertices */
    val t1 = E.map(e => e.edge.u)
    val t2 = E.map(e => e.edge.v)
    val allVertices: RDD[Int] = t1.union(t2).distinct // remove duplicate vertices

    /** Initialize Redis Disjoint Set */
    val r = new Jedis()
    r.flushAll()
    r.mset("components", allVertices.count.toString)
    r.close()
    redisDisjointSet(allVertices)
   
    BoruvkaLoop(allVertices.map(v => v -> v), E, spark.sparkContext.emptyRDD[weightedEdge])
  }
}
object ord extends Ordering[weightedEdge] {
  override def compare(x: weightedEdge, y: weightedEdge): Int = y.weight.compareTo(x.weight)
}
