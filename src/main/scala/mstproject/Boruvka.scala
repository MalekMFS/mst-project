package mstproject

object Boruvka {
  def apply(E: Array[((Int, Int), Int)]) : Array[((Int, Int), Int)] ={
    val s = new DisjoIntSet[Int]
    val tree = new Array[((Int, Int), Int)](0)
    // making new sets: O(E)
    for (e <- E){
        val u = e._1._1
        val v = e._1._2

        if(s.find(u) == -1)
            s.add(u)
        if(s.find(v) == -1)
            s.add(v)
    }
    while(s.size > 1){
        
    }
    // start vertex
    val start = 1
    // val m = new Map[Int, List[((Int, Int), Int)]](s.size)

    return tree
    }
}
