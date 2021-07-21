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
    selectedEdges.foreachPartition( part => part.foreach( e => RedisDisjointSet.union(e.edge.u, e.edge.v) ))

  @tailrec
  def BoruvkaLoop(setToE:  RDD[(Int, Iterable[weightedEdge])], forest: RDD[weightedEdge], debug: Boolean): List[weightedEdge] = {
    val round = RedisDisjointSet.iterationInc
    log.warn(s"*** iteration $round ****")
    val components = RedisDisjointSet.componentsCount
    log.warn(s"# components: $components")
    if(debug) { val mstCount = forest.count; log.warn(s"# input MST edges: $mstCount") }

    if (components > 1) {
      /** foreach connected component select their minimum edge */
      val VToECandidates = setToE.mapPartitions{ part =>
        part.map{ vEs =>
          val iteration: Int = RedisDisjointSet.iterationCount
          val vertex = vEs._1 // or set after the first round
          val selectableEdges = vEs._2.filter(e => !e.removed && RedisDisjointSet.find(e.edge.u) != RedisDisjointSet.find(e.edge.v))
          if (selectableEdges.nonEmpty){
            val minEdge = selectableEdges.minBy(_.weight)
            val updatedEdges  = vEs._2.groupBy(_.id).values.map(it => it.last) // remove duplicate edges
              .map { e =>
                if (!e.removed && e == minEdge) // and not in same set already checked
                  if (iteration > 1)
                    weightedEdge(e.edge, e.weight, removed = false, selectedInStep = iteration, selectedByV = vertex, e.id)
                  else // First iteration: sets = vertices. deterministic choices.
                  {
//                    log.warn(s"min is: ${minEdge.id}: ${minEdge.edge.u + "-" + minEdge.edge.v} w: ${minEdge.weight}")
                    weightedEdge(e.edge, e.weight, removed = true, selectedInStep = iteration, selectedByV = vertex, e.id)
                  }
                else e
              }
            (vertex, updatedEdges)
          }
          else (vertex, vEs._2.map(e => weightedEdge(e.edge, e.weight, removed = true, e.selectedInStep, e.selectedByV, e.id))) // mark all edges of a vertex as removed because they were in same set or already removed
        }
      } //.cache
      // TODO partition sets by vertices.
      if(debug) { val numCandidates = VToECandidates.flatMap(_._2).filter(_.selectedInStep == round).count; log.warn(s"# Candidate edges to be removed (selected): $numCandidates") }

      val updatedSetToE: RDD[(Int, Iterable[weightedEdge])] = {
        if (round > 1)
          VToECandidates.groupBy(x => RedisDisjointSet.find(x._1)).map { sets =>
            val set = sets._1
            val edgesOfVertex = sets._2
            val selectableEdges = edgesOfVertex.flatten(_._2)
              .groupBy(_.id).values.map(it => it.last) // remove duplicate edges
              .filter(_.selectedInStep == round)
            if (selectableEdges.nonEmpty){
              val setMinEdge = selectableEdges.minBy(_.weight)
              val res = edgesOfVertex.flatMap{ thisSet =>
                val edges = thisSet._2
                edges.map { e =>
                  if (!e.removed && e == setMinEdge)
                    weightedEdge(e.edge, e.weight, removed = true, e.selectedInStep, e.selectedByV, e.id)
                  else e
                }
              }.groupBy(_.id).values.map(it => it.last) // remove duplicate edges
              (set.getOrElse(-1L).toInt, res)
            } else {
              val markedRemoved = edgesOfVertex.flatten(_._2).map(e => weightedEdge(e.edge, e.weight, removed = true, e.selectedInStep, e.selectedByV, e.id))
              (set.getOrElse(-1L).toInt, markedRemoved)
            }
          }
        else VToECandidates
      }.cache
      if(debug){ val numKeys = updatedSetToE.count; log.warn(s"# Keys (should be same as components): $numKeys") }

      val selectedEdges = updatedSetToE.flatMap(_._2).filter(e => e.removed && e.selectedInStep == round)
        .groupBy(_.id).values.map(it => it.last) // remove duplicate edges
        .cache
      if(debug) {
        val selectedEdgesCount = selectedEdges.count(); log.warn(s"# selected edges: $selectedEdgesCount")
        val removedCount = updatedSetToE.flatMap(_._2).filter(_.removed).count; log.warn(s"# Total removed edges: $removedCount")
      }

      /** union sets in this iteration */
        updateRedisDisjointSet(selectedEdges) //NOTE: an action will be called on it
        log.warn("DisjointSet updated")

      BoruvkaLoop(updatedSetToE, forest ++ selectedEdges, debug) // next iteration after updates
    }
    else { // return the resulting forest
      val mstWeight = forest.aggregate(0)((acc, e) => acc + e.weight, (acc, e) => acc + e)
      log.warn(s"Weight of MST: $mstWeight")
      log.warn("Finished. Now Collecting...")
      forest.collect().toList
    }
  }

  def apply(path: String, delim: String, weighted: Boolean = false, debug: Boolean = false): List[weightedEdge] = {
    //TODO anywhere needed to cache data?

    lazy val spark: SparkSession = SparkConstructor()
    lazy val sc = spark.sparkContext
//    val E: RDD[weightedEdge] = sc.parallelize(edges, 100) // number of partitions

    val E: RDD[weightedEdge] = if (weighted)
      spark.read
        .format("csv").option("delimiter",delim)
        .load(path).toDF("u", "v", "w")
        .rdd
        .zipWithIndex
        .map( r => weightedEdge( myEdge(r._1.getAs[String](0).toInt, r._1.getAs[String](1).toInt), r._1.getAs[String](2).toInt, id = r._2.toInt) )
      else
      spark.read
        .format("csv").option("delimiter",delim)
        .load(path).toDF("u", "v")
        .rdd
        .zipWithIndex // add weights
        .map( r => weightedEdge( myEdge(r._1.getAs[String](0).toInt, r._1.getAs[String](1).toInt), r._2.toInt, id = r._2.toInt) )
//    .cache
    log.warn(s"# Graph edges: ${E.count}")

    /** Find all vertices */
    val t1 = E.map(e => e.edge.u)
    val t2 = E.map(e => e.edge.v)
    val allVertices: RDD[Int] = t1.union(t2).distinct // removes duplicate vertices
    val vCount = allVertices.count; log.warn(s"# Graph vertices: $vCount")

    /** Initialize Redis Disjoint Set */
    RedisDisjointSet.flush
    RedisDisjointSet.componentsSet(vCount)
    RedisDisjointSet(allVertices)


    /** Vertex to List of Edges map, and list of vertices */
    val t3 = E.map(e => vertexEdge(e.edge.u, e))
    val t4 = E.map(e => vertexEdge(e.edge.v, e))
    val vToE = t3.union(t4)
      .groupBy(kv => kv.v)
      .mapValues(list => list.map(_.edge))

    val result = BoruvkaLoop(vToE, spark.sparkContext.emptyRDD[weightedEdge], debug)
    log.warn("Collected Result Successfully!")
    result
  }
}