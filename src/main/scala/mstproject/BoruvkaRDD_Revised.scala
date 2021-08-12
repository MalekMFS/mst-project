package mstproject

import mstproject.Model._
import org.apache.log4j.Logger
import org.apache.spark.Partitioner
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

    println("partitions: " + setToE.getNumPartitions)
    if (components > 1) {
      /** foreach connected component select their minimum edge */
      val VToECandidates: RDD[(Int, Iterable[weightedEdge])] = setToE
//        .partitionBy(new Partitioner {
//          override def numPartitions: Int = 100
//
//          override def getPartition(key: Any): Int = {
//            key.asInstanceOf[Int] % numPartitions
//          }
//        })
        .mapPartitions{ part =>
        part.map{ vEs =>
          val iteration: Int = RedisDisjointSet.iterationCount
          val vertex = vEs._1 // or set after the first round
          val selectableEdges = if(round > 1) vEs._2.filter(e => !e.removed && RedisDisjointSet.find(e.edge.u) != RedisDisjointSet.find(e.edge.v)) else vEs._2
          if (selectableEdges.nonEmpty){
            val minEdge = selectableEdges.minBy(_.weight) // of set (or vertex in first round)
            val updatedEdges  = vEs._2.groupBy(_.id).values.map(it => it.last) // remove duplicate edges inside the set
              .map { e =>
                if (!e.removed && e.id == minEdge.id) // and not in same set already checked
                  if (iteration > 1)
                    weightedEdge(e.edge, e.weight, removed = false, selectedInStep = iteration, e.id)
                  else // First iteration: sets = vertices. deterministic choices.
                  {
//                    log.warn(s"min is: ${minEdge.id}: ${minEdge.edge.u + "-" + minEdge.edge.v} w: ${minEdge.weight}")
                    weightedEdge(e.edge, e.weight, removed = true, selectedInStep = iteration, e.id)
                  }
                else e
              }
            (vertex, updatedEdges)
          }
          else (vertex, vEs._2.map(e => weightedEdge(e.edge, e.weight, removed = true, e.selectedInStep, e.id))) // mark all edges of a vertex as removed because they were in same set or already removed
        }
      } //.cache
      // TODO partition sets by vertices.
      if(debug) { val numCandidates = VToECandidates.flatMap(_._2).filter(_.selectedInStep == round).count; log.warn(s"# Candidate and removed edges in this round: $numCandidates") }

      val updatedSetToE = {
        if (round > 1) { // then some of the candidate edges are not the min edge of their set
          //FIXME: should also merge sets after first round
          VToECandidates.mapPartitions(part => part.map(c => (RedisDisjointSet.find(c._1), c._2))).groupBy(x => x._1)
//            .partitionBy(new Partitioner {
//              override def numPartitions: Int = 50
//
//              override def getPartition(key: Any): Int = {
//                key.asInstanceOf[Int] % numPartitions
//              }
//            })
            .map { sets => // merge sets to make a new
            val set = sets._1
            val edgesOfVertex = sets._2
            val selectableEdges = edgesOfVertex.flatten(_._2)
              .filter(e => !e.removed && e.selectedInStep == round) // Candidates
              .groupBy(_.id).values.map(it => it.last) // remove duplicate edges //FIXME redundant?
            if (selectableEdges.nonEmpty){
              val setMinEdge = selectableEdges.minBy(_.weight)
              val res = edgesOfVertex.flatMap{ thisSet =>
                val edges = thisSet._2
                edges.map { e =>
                  if (!e.removed && e.id == setMinEdge.id)
                    weightedEdge(e.edge, e.weight, removed = true, e.selectedInStep, e.id)
                  else e
                }
              }.groupBy(_.id).values.map(it => it.last) // remove duplicate edges
              (set.getOrElse(-1L).toInt, res) //FIXME do I need to handle -1 set?
            } else {
              //TODO just filter the removed edges?
              val markedRemoved = edgesOfVertex.flatten(_._2).map(e => weightedEdge(e.edge, e.weight, removed = true, e.selectedInStep, e.id))
              (set.getOrElse(-1L).toInt, markedRemoved)
            }
          }
        } else VToECandidates
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
      val mstWeight = forest.aggregate(0L)((acc, e) => acc + e.weight, (acc, e) => acc + e)
      log.warn(s"Weight of MST: $mstWeight")
      log.warn("Finished. Now Collecting...")
      forest.collect().toList
    }
  }

  def apply(dataset: graphDataset, debug: Boolean = false): List[weightedEdge] = {
    //TODO anywhere needed to cache data?

    lazy val spark: SparkSession = SparkConstructor()
    lazy val sc = spark.sparkContext
//    val E: RDD[weightedEdge] = sc.parallelize(edges, 100) // number of partitions

    val E: RDD[weightedEdge] = if (dataset.weighted)
      spark.read
        .format("csv").option("delimiter",dataset.delim)
        .load(dataset.path).toDF("u", "v", "w")
        .rdd
        .zipWithIndex
        .map( r => weightedEdge( myEdge(r._1.getAs[String](0).toInt, r._1.getAs[String](1).toInt), r._1.getAs[String](2).toInt, id = r._2.toInt) )
      else
      spark.read
        .format("csv").option("delimiter",dataset.delim)
        .load(dataset.path).toDF("u", "v")
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
    val vToE: RDD[(Int, Iterable[weightedEdge])] = t3.union(t4)
      .groupBy(kv => kv.v)
      .mapValues(list => list.map(_.edge))
//      .partitionBy(new Partitioner {
//      override def numPartitions: Int = vCount.toInt
//
//      override def getPartition(key: Any): Int = {
//        key.asInstanceOf[Int]
//      }
//    })

    val result = BoruvkaLoop(vToE, spark.sparkContext.emptyRDD[weightedEdge], debug)
    log.warn("Collected Result Successfully!")
    result
  }
}