package mstproject

import Model._

import scala.annotation.tailrec

/** Returns MST for Connected graphs */
object Boruvka {
//  def demo[T: Monoid](ls: List[T]) =
//    ls.foldLeft(Monoid[T].empty) (_ |+| _)

  def apply(E : List[weightedEdge]): List[weightedEdge] = {

    /** Vertex -> List of Edges map, and list of vertices */
    val t1 = E.map(e => (e.edge.u, e))
    val t2 = E.map(e => (e.edge.v, e))
    val vToE = (t1 ++ t2).groupBy(_._1).mapValues(list => list.map(_._2)) // a map from Vertex to Edges connected to that vertex

    /** Initialize the DisjointSet. add a set for each vertex */
    val s = tikiDisjointSet[Int](vToE.keys.toSet)

    /** While 'set' has more than one component */
    @tailrec
    def BoruvkaLoop (vSet: tikiDisjointSet[Int], edgesMap: Map[Int, List[weightedEdge]], forest: List[weightedEdge]): List[weightedEdge] =

      if (vSet.components > 1) { // Connected Graph
        val sets = vSet.parents
          .map(x => (x._1, vSet.find(x._2))) // Parents could contain parents that are not self-parent.
          .groupBy(_._2)
          .mapValues(_.keys) // a Map from Sets to corresponding vertices
        /** foreach connected component select their minimum edge */
        val selectedEdges = sets.map { set =>
          val vertices = set._2
          // 1. Find cheapest outgoing edge (for each set)
          // 2. select the cheapest among them
          vertices.flatMap( v =>
            edgesMap(v)
              .filter(e => vSet.find(e.edge.u) != vSet.find(e.edge.v))
            //FIXME: still it could be empty and minBy complains
          ).minBy(_.weight) // Reducing by minBy
        }.toSet // removed duplicate edges by toSet conversion

        /** Remove selected edges from the edgesMap for the next iterations */
        //FIXME `fold` instead of `foldLeft`. foldLeft is slow for big collections
        val newEdgesMap = selectedEdges.foldLeft(edgesMap) { (m, e) =>
          val tempMap = m.-(e.edge.u, e.edge.v)
          val eu = m(e.edge.u)  diff List(e)
          val ev = m(e.edge.v)  diff List(e)
          /** re-add vertices with new edges */
          if (eu.nonEmpty && ev.nonEmpty)
            tempMap + (e.edge.u -> eu, e.edge.v -> ev)
          else if (eu.nonEmpty)
            tempMap + (e.edge.u -> eu, e.edge.v -> List[weightedEdge]())
          else if (ev.nonEmpty)
            tempMap + (e.edge.v -> ev, e.edge.u -> List[weightedEdge]())
          else
            tempMap + (e.edge.u -> List[weightedEdge](), e.edge.v -> List[weightedEdge]())
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

    BoruvkaLoop(s, vToE, List[weightedEdge]())
  }
}
