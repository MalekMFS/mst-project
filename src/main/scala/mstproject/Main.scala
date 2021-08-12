// https://www.jetbrains.com/help/idea/creating-a-remote-server-configuration.html#mapping
package mstproject

import better.files._
import mstproject.Model._
import redis.clients.jedis.Jedis

object Main extends App {
    //TODO use Iterator instead of List for memory efficiency.
    val datasets = List(("src/main/resources/out.moreno_train_train", " ", true, 64, 243), ("src/main/resources/facebook_combined.txt", " ", false, 4039, 88234), ("src/main/resources/roadNet-CA.txt", "\t" , false, 1965206, 2766607), ("src/main/resources/as-skitter.txt", "\t", false, 1696415, 11095298), ("src/main/resources/com-orkut.ungraph.txt", "\t", false, 3072441, 117185083))
      .map(d => graphDataset(d._1, d._2, d._3, d._4, d._5))
    lazy val sampleEdges = List(((1,4),14), ((3,5),5), ((4,6),6), ((1,2),7), ((2,5),3), ((2,3),8), ((5,6),10), ((2,4),11), ((5,7),12), ((6,7),13), ((4,5),15))
    lazy val morenoEdges = file"${datasets(0).path}".lines.map( l => l.split(datasets(0).delim).map(_.toInt) ).toList
    lazy val fbEdges = file"${datasets(1).path}".lines.map( l => l.split(datasets(1).delim).map(_.toInt) ).toList
    lazy val caEdges = file"${datasets(2).path}".lines.map( l => l.split(datasets(2).delim).map(_.toInt) ).toList
    lazy val skitter = file"${datasets(3).path}".lines.map( l => l.split(datasets(3).delim).map(_.toInt) ).toList
    lazy val orkutEdges = file"${datasets(4).path}".lines.map( l => l.split(datasets(4).delim).map(_.toInt) ).toList

//    val edges = sampleEdges.zipWithIndex.map(r => weightedEdge( myEdge(r._1._1, r._1._2), r._2, id = r._2))
//    val edges = morenoEdges.zipWithIndex.map(r => weightedEdge( myEdge(r._1(0), r._1(1)), r._1(2), id = r._2))
//    val edges = fbEdges.zipWithIndex.map(r => weightedEdge( myEdge(r._1(0), r._1(1)), r._2, id = r._2))
//    val edges = caEdges.zipWithIndex.map(r => weightedEdge( myEdge(r._1(0), r._1(1)), r._2, id = r._2 ))
//    val edges = orkutEdges.zipWithIndex.map(r => weightedEdge( myEdge(r._1(0), r._1(1)), r._2, id = r._2 ))

//    println(s"Number of edges= ${edges.length}")
    val startTime = System.nanoTime

//    val kruskal = Kruskal(edges)
//    println("Kruskal: " + kruskal.length)//; kruskal.foreach(println)
//    println("---")
//    val prim = Prim(edges)
//    println("Prim: " + prim.length)//; prim.foreach(println)
//    println("---")
//    val boruvka = Boruvka(edges)
//    println("Boruvka: " + boruvka.length)//; boruvka.foreach(println)
    val boruvkaRDD_Revised = BoruvkaRDD_Revised(datasets(2), debug = false)
    println("BoruvkaRDD_Revised: " + boruvkaRDD_Revised.length)//; boruvkaRDD_Revised.foreach(println)
//    val alg3 = CME323.algorithm3(datasets(4))
//    println("alg3: " + alg3.length)//; alg3.foreach(println)
//    val boruvkaGraphX = BoruvkaGraphX(datasets(2))
//    println("BoruvkaGraphX: " + boruvkaGraphX.length)//; boruvkaGraphX.foreach(println)

    val duration = (System.nanoTime - startTime) / 1e9d // show in seconds
    println("Duration: " + duration)
}