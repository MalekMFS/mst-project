package mstproject

import mstproject.Model._
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.Jedis

import scala.annotation.tailrec

object BoruvkaRDD_Revised {
  @transient lazy val log: Logger = org.apache.log4j.LogManager.getLogger("myLogger")

  /** Updates DisjointSet */
  def updateRedisDisjointSet(selectedEdges: RDD[weightedEdge]): Unit =
    selectedEdges.foreachPartition( part => part.foreach( e => (e, RedisDisjointSet.union(e.edge.u, e.edge.v))))

  @tailrec
  def BoruvkaLoop(vToE:  RDD[(Int, Iterable[weightedEdge])], forest: RDD[weightedEdge]): List[weightedEdge] = {
    val itCount = RedisDisjointSet.iterationInc
    log.warn(s"*** iteration $itCount ****")
    val components = RedisDisjointSet.componentsCount
    log.warn(s"# components: $components")
    val mstCount = forest.count
    log.warn(s"# MST count: $mstCount")

    if (components > 1) {
      /** foreach connected component select their minimum edge */
      val updatedVToE = vToE.mapPartitions{ part => //FIXME: selected edges in a
        part.map{ vEs =>
          val iteration: Int = RedisDisjointSet.iterationCount
          val vertex = vEs._1
          val cleanEdges = vEs._2.filter(e => !e.removed && RedisDisjointSet.find(e.edge.u) != RedisDisjointSet.find(e.edge.v)) //FIXME: redundant redis lookup
          if (cleanEdges.nonEmpty){
            val minEdge = cleanEdges.minBy(_.weight)
            val updatedEdges  = vEs._2
              .map { e =>
                if (!e.removed && RedisDisjointSet.find(e.edge.u) != RedisDisjointSet.find(e.edge.v))
                  if(e == minEdge) weightedEdge(e.edge, e.weight, removed = true, iteration) else e
                else e
              }
            (vertex, updatedEdges)
          }
          else vEs
        }
      }.cache
      // TODO partition sets by vertices.

      val removedCount = updatedVToE.flatMap(_._2).filter(_.removed).count
      log.warn(s"# removed edges: $removedCount")

      //FIXME: what about edges choosing the same edge but seems twice?
      val selectedEdges = updatedVToE.flatMap(_._2).filter(_.selectedInStep == RedisDisjointSet.iterationCount)//FIXME: iteration number
        .cache
      val selectedEdgesCount = selectedEdges.count()
      log.warn(s"# selected edges: $selectedEdgesCount")

      /** union sets in this iteration */
      updateRedisDisjointSet(selectedEdges) //NOTE: an action will be called on it
      log.warn("Disjointset updated")
      // TODO: mark inner vertices as removed

      BoruvkaLoop(updatedVToE, forest ++ selectedEdges) // next iteration after updates
    }
    else { // return the resulting forest
      log.warn("Finished. Now Collecting...")
      forest.collect().toList
    }
  }

  def apply(path: String, delim: String): List[weightedEdge] = {
    //TODO anywhere needed to cache data?

    lazy val spark: SparkSession = SparkConstructor()
    val E = spark.read
      .format("csv").option("delimiter",delim)
      .load(path).toDF("u", "v")
      .rdd
      .zipWithIndex // add weights
      .map( r => weightedEdge( myEdge(r._1.getAs[String](0).toInt, r._1.getAs[String](1).toInt), r._2.toInt) )
      .cache
//    val sc = spark.sparkContext
    log.warn(s"# edges: ${E.count}")

    /** Find all vertices */
    val t1 = E.map(e => e.edge.u)
    val t2 = E.map(e => e.edge.v)
    val allVertices: RDD[Int] = t1.union(t2).distinct // removes duplicate vertices
    val vCount = allVertices.count
    log.warn(s"# vertices: $vCount")

    /** Initialize Redis Disjoint Set */
    val r = new Jedis()
    r.flushAll()
    r.mset("components", vCount.toString)
    r.mset("iteration", "0")
    r.close()
    RedisDisjointSet(allVertices)


    /** Vertex to List of Edges map, and list of vertices */
    val t3 = E.map(e => vertexEdge(e.edge.u, e))
    val t4 = E.map(e => vertexEdge(e.edge.v, e))
    val vToE = t3.union(t4)
      .groupBy(kv => kv.v)
      .mapValues(list => list.map(_.edge))

    val result = BoruvkaLoop(vToE, spark.sparkContext.emptyRDD[weightedEdge])
    log.warn("Collected Result Successfully!")
    result
  }
}