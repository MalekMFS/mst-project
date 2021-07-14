package mstproject
// by swaroop et al
// refactored by MFS
import mstproject.Model._

import scala.collection.mutable.ListBuffer

object Kruskal {
  def apply(E : List[weightedEdge]): List[weightedEdge] = {
    val ESorted = E.sortBy(_.weight)

    var tree = ListBuffer[weightedEdge]()
    val x = new DisjointSet[Int]


    for(e <- ESorted)
    {
        val u = e.edge.u
        val v = e.edge.v

        if(x.find(u) == -1)
            x.add(u)
        if(x.find(v) == -1)
            x.add(v)

        if(x.find(u) != x.find(v))
        {
            tree += e
            x.union(u,v)
        }
    }

//    for(e <- tree)
//    {
//        val u = e.edge.u
//        val v = e.edge.v
//        val w = e.weight
//    }

    return tree.toList
    }
}
