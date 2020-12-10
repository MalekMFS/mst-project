package mstproject

import Model._

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

object Boruvka {
//  def demo[T: Monoid](ls: List[T]) =
//    ls.foldLeft(Monoid[T].empty) (_ |+| _)

  def apply(E : List[weightedEdge]): List[weightedEdge] = {
    //TODO use bloomFilter
    //FIXME tikiDisjointSet's find method seems non-optimal. fix or replace it.

    /** Vertex -> List of Edges map, and list of vertices */
    val t1 = E.map(e => (e.edge.u, e))
    val t2 = E.map(e => (e.edge.v, e))
    val vToE = (t1 ++ t2).groupBy(_._1).mapValues(list => list.map(_._2)) // a map from Vertex to Edges connected to vertex
    val vertices = ListBuffer[Int]()

    /** Initialize the DisjointSet. add a set for each vertex */
    for (e <- E){
      val u = e.edge.u
      val v = e.edge.v

      if(!vertices.contains(u))
        vertices += u
      if(!vertices.contains(v))
        vertices += v
    }
    val s = tikiDisjointSet[Int](vertices.toSet)

    /** While 'set' has more than one component */
    @tailrec
    def BoruvkaLoop (vSet: tikiDisjointSet[Int], edgesMap: Map[Int, List[weightedEdge]], forest: List[weightedEdge]): List[weightedEdge] = {
      //TODO check if edgesMap is nonempty for disconnected graph
      if (vSet.components > 1) {
        val sets = vSet.parents.groupBy(_._2).mapValues(_.keys) // a Map from Sets to corresponding vertices
        /** foreach connected component select their minimum edge */
        val selectedEdges = sets.map { set =>
          val vertices = set._2
          // 1. Find cheapest outgoing edge (for each set)
          // 2. select the cheapest among them
          vertices.flatMap( v =>
            edgesMap(v)
              .filter(e => vSet.find(e.edge.u) != vSet.find(e.edge.v))
          ).minBy(_.weight) // Reducing by minBy
        }.toSet // removed duplicate edges by toSet conversion

        /** Remove selected edges from the edgesMap for the next iterations */
        //TODO check if it's better to use `fold` instead of `foldLeft`
        val newEdgesMap = selectedEdges.foldLeft(edgesMap) { (m, e) =>
          //TODO any special case that temp.length == 0 ?
          val temp = m.-(e.edge.u, e.edge.v)
          val eu = m(e.edge.u)  diff List(e)
          val ev = m(e.edge.v)  diff List(e)
          if (eu.nonEmpty && ev.nonEmpty)
            temp + (e.edge.u -> eu, e.edge.v -> ev)
          else if (eu.nonEmpty)
            temp + (e.edge.u -> eu)
          else if (ev.nonEmpty)
            temp + (e.edge.v -> ev)
          else
            temp
        }

        /** union sets in this iteration */
        val newvSet: tikiDisjointSet[Int] =
          selectedEdges.foldLeft(vSet)((s, e) => s.union(e.edge.u, e.edge.v)
          match {
            case Some(set) => set
            case None => tikiDisjointSet.empty
          })

        BoruvkaLoop(newvSet, newEdgesMap, forest ++ selectedEdges.toList)
      }
      else forest
    }

    BoruvkaLoop(s, vToE, List[weightedEdge]())
  }
}
