// https://www.jetbrains.com/help/idea/creating-a-remote-server-configuration.html#mapping
package mstproject

import better.files._
import mstproject.Model._
import redis.clients.jedis.Jedis

object Main extends App {
    //TODO use Iterator instead of List for memory efficiency.
    lazy val sampleEdges = List(((1,4),14), ((3,5),5), ((4,6),6), ((1,2),7), ((2,5),3), ((2,3),8), ((5,6),10), ((2,4),11), ((5,7),12), ((6,7),13), ((4,5),15))
    lazy val morenoEdges = file"src/main/resources/out.moreno_train_train".lines.map( l => l.split(" ").map(_.toInt) ).toList
    lazy val fbEdges = file"src/main/resources/facebook_combined.txt".lines.map( l => l.split(" ").map(_.toInt) ).toList
    lazy val caEdges = file"src/main/resources/roadNet-CA.txt".lines.map( l => l.split("\t").map(_.toInt) ).toList

//    val edges = sampleEdges.zipWithIndex.map(r => weightedEdge( myEdge(r._1._1, r._1._2), r._2, id = r._2))
//    val edges = morenoEdges.zipWithIndex.map(r => weightedEdge( myEdge(r._1(0), r._1(1)), r._1(2), id = r._2))
//    val edges = fbEdges.zipWithIndex.map(r => weightedEdge( myEdge(r._1(0), r._1(1)), r._2, id = r._2))
    val edges = caEdges.zipWithIndex.map(r => weightedEdge( myEdge(r._1(0), r._1(1)), r._2, id = r._2 ))

    println(s"Number of edges= ${edges.length}")
//    val kruskal = Kruskal(edges)
//    println("Kruskal: " + kruskal.length)//; kruskal.foreach(println)
//    println("---")
//    val prim = Prim(edges)
//    println("Prim: " + prim.length)//; prim.foreach(println)
//    println("---")
//    val boruvka = Boruvka(edges)
//    println("Boruvka: " + boruvka.length)//; boruvka.foreach(println)
    val boruvkaRDD_Revised = BoruvkaRDD_Revised(edges)
    println("BoruvkaRDD_Revised: " + boruvkaRDD_Revised.length)//; boruvkaDS.foreach(println)

//    val boruvkaRDD_RS = BoruvkaRDD_RS("src/main/resources/facebook_combined.txt", " ")
//    println("BoruvkaRS: " + boruvkaRDD_RS.length)//; boruvkaDS.foreach(println)
//    val boruvkaRDD_Revised = BoruvkaRDD_Revised("src/main/resources/out.moreno_train_train", " ", weighted = true)
//    val boruvkaRDD_Revised = BoruvkaRDD_Revised("src/main/resources/facebook_combined.txt", " ")
//    println("BoruvkaRDD_Revised: " + boruvkaRDD_Revised.length)//; boruvkaDS.foreach(println)
//    val boruvkaGraphX = BoruvkaGraphX("src/main/resources/facebook_combined.txt", " ")
//    println("BoruvkaGraphX: " + boruvkaGraphX.length)//; boruvkaGraphX.foreach(println)
}