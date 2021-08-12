package mstproject

import mstproject.Model._
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.GraphLoader

object ConnectedComponents {
  // Returns number of connected components
  def apply(dataset: graphDataset): Long = {
    lazy val spark: SparkSession = SparkConstructor()
    lazy val sc = spark.sparkContext

    val graph = GraphLoader.edgeListFile(sc, dataset.path)
    val verticesComponents = graph.connectedComponents().vertices
    verticesComponents.collect.map(_._2).toSet.size.toLong //.map(_._2).collect.toSet
  }
}
