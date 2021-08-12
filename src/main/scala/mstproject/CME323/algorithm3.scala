package mstproject.CME323
// an Implementation of CME-323.algorithm3 (Parallel Prim) of  "s-ramaswamy"
// Main source: https://github.com/s-ramaswamy/CME-323-project/blob/master/algorithm3.scala
import mstproject.Model.{graphDataset, myEdge, weightedEdge}
import mstproject.SparkConstructor
import org.apache.log4j.Logger

import scala.collection.mutable.Map
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.language.postfixOps
import scala.math
import scala.util.control._

class DisjointSet[Element] extends Serializable{

  val parent = new HashMap[Element, Element]
  val rank = new HashMap[Element, Int]

  /* number of Elements in the Data structure */
  def size = parent.size

  /* Add an Element to the collection */
  def +=(x: Element) = add(x)
  def ++(x: Element) = add(x)

  def add(x: Element) {
    parent += (x -> x)
    rank += (x -> 0)
  }

  /* Union of two Sets which contain x and y */
  def union(x: Element, y: Element) {
    val s = find(x)
    val t = find(y)
    if(s == t) return
    if(rank(s) > rank(t)) parent += (t -> s)
    else{
      if (rank(s) == rank(t)) rank(t) += 1
      parent += (s -> t)
    }
  }

  /* Find the set/root of the tree containing given Element x */
  def find(x: Element): Element = {
    if(parent(x) == x) x
    else{
      parent += (x -> find(parent(x)))
      parent(x)
    }
  }

  /* check the connectivity between two Elements */
  def isConnected(x: Element, y:Element): Boolean = find(x) == find(y)

  /* toString method */
  override def toString: String = parent.toString
}

object algorithm3 {
  @transient lazy val log: Logger = org.apache.log4j.LogManager.getLogger("myLogger")
  def apply(dataset: graphDataset): List[weightedEdge] = {
    val spark = SparkConstructor()
    val sc = spark.sparkContext
    val V = 0 to dataset.vCount  toArray
    val distK = sc.textFile(dataset.path)
    log.warn("Read the dataset")
    val distE = distK.map( s => ((s.split(dataset.delim)(0).toInt,s.split(dataset.delim)(1).toInt), if(dataset.weighted) s.split(dataset.delim)(2).toInt else 1) )
    var A = new DisjointSet[Int]
    for(i <- V){
      A.add(i)
    }
    var distEgrouped = distE.map(x => (scala.util.Random.nextInt(4), x)).groupByKey
    var MST:Set[((Int,Int),Int)] = Set()
    val niter: Int =(math.log(V.size)/math.log(2)).ceil.toInt // log in base-2 (ln)
    log.warn(s"niter (max iteration?): $niter")
    var edgelistsize = 1
    for(m <- 1 to niter if edgelistsize != 0){
      log.warn(s"*** iteration $m ****")
      var B:scala.collection.immutable.HashMap[Int,Int] = scala.collection.immutable.HashMap()
      for(i <- V){
        B += (i -> A.find(i))
      }

      var xbroad = sc.broadcast(B)
      log.warn(s"Broadcasted new DisjointSet")
      var tmp = distEgrouped.map(a => findminimum(mapfunction(a._2), xbroad.value)) // RDD map operation
      log.warn(s"Mapped for minimum edges")
      var edgelist = tmp.reduce((a,b) => reduceMaps(a,b)) // RDD reduce operation
      log.warn(s"Reduced to minimum edges")
      edgelistsize = edgelist.size
      log.warn(s"EdgeListSize: $edgelistsize")
      for((key,value) <- edgelist){
        A.union(value._1._1, value._1._2)
        MST += value
      }
    }
    log.warn(s"MST Size: ${MST.size}")
    MST.map( e => weightedEdge(myEdge(e._1._1, e._1._2), e._2, id = -1) ).toList
  }

  def find(x: Int, parent: HashMap[Int,Int]): Int = {
    if(parent(x) == x) x
    else{
      parent += (x -> find(parent(x), parent))
      parent(x)
    }
  }
  def mapfunction(x: Iterable[((Int,Int),Int)]): Array[((Int,Int), Int)] = {
    var y = new Array[((Int,Int), Int)](x.size)
    x.copyToArray(y)
    return y
  }

  def findminimum(elist: Array[((Int,Int),Int)], xbroad: scala.collection.immutable.HashMap[Int,Int]): Map[Int,((Int,Int),Int)] = {
    var y:Map[Int,((Int,Int),Int)]  = Map()
    for(e <- elist)
    {
      val u = e._1._1
      val v = e._1._2
      val wt = e._2
      val compU = xbroad(u)
      val compV = xbroad(v)
      if(compU != compV)
      {
        if(!y.contains(compU))
          y += (compU -> e)

        if(!y.contains(compV))
          y += (compV -> e)

        if(y(compU)._2 > wt)
          y(compU) = e

        if(y(compV)._2 > wt)
          y(compV) = e
      }
    }
    return y
  }

  def minfunction(x: ((Int,Int), Int), y: ((Int,Int), Int)): ((Int, Int), Int) = {
    if(x._2 > y._2 && y._2 != -1)
      return y
    return x
  }

  def reduceMaps(p: Map[Int,((Int,Int),Int)], q: Map[Int,((Int,Int),Int)]): Map[Int,((Int,Int),Int)] = {
    var z = p ++ q.map{case (k,v) => k -> minfunction(v,  p.getOrElse(k, ((0,0),-1)))}
    return z
  }

}
