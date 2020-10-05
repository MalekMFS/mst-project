package mstproject

import Model._

object Boruvka {
  def apply(E : List[weightedEdge]): List[weightedEdge] = {

    val s = new DisjoIntSet[Int]
//    val tree = List[weightedEdge]()
    // Initialize the set. making new sets: O(E)
    for (e <- E){
      val u = e.edge.u
      val v = e.edge.v

      if(s.find(u) == -1)
          s.add(u)
      if(s.find(v) == -1)
          s.add(v)
    }
    /** While 's' has more than one component */
    while(s.size > 1){
        
    }
    // start vertex
    val start = 1
    // val m = new Map[Int, List[((Int, Int), Int)]](s.size)

    return tree
    }
}
