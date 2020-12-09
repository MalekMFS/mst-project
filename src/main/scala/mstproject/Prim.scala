package mstproject

import mstproject.Model._

import scala.collection.mutable.ListBuffer

object Prim {
    //TODO refactor to FP. you could use Map-Filter instead of Loop-if. Reduce mutation.
    //TODO Check Parallel algorithm in wikipedia
    //TODO use DisjointSet instead of Set? cost? limitations? extension?
    def apply(E: List[weightedEdge]): List[weightedEdge] ={

        object MinOrder extends Ordering[weightedEdge] {
            override def compare(x: weightedEdge, y: weightedEdge): Int = y.weight.compareTo(x.weight)
        }

        val t1 = E.map(e => (e.edge.u, e))
        val t2 = E.map(e => (e.edge.v, e))
        //FIXME: rewrite it with a map of vertex to List[WeightedEdge]:
        // (t1 ++ t2).groupBy(_._1).mapValues(list => list.map(_._2))
        val vToE = (t1 ++ t2).groupBy(_._1) // a map from Vertex to connected Edges
        val minHeap = scala.collection.mutable.PriorityQueue.empty(MinOrder)
        var metVertices = Set[Int]()
        var tree = ListBuffer[weightedEdge]()
        
        val startV = E.head.edge.u
        metVertices += startV
        minHeap ++= vToE(startV).map(_._2)
        var minEdge = minHeap.dequeue

        for( _ <- 1 until vToE.size){
            //TODO any better way to deal with rest of the edges?
            while (metVertices(minEdge.edge.u) && metVertices(minEdge.edge.v))
                minEdge = minHeap.dequeue
            val u = minEdge.edge.u
            val v = minEdge.edge.v

            if(metVertices(u) && !metVertices(v))
            {
                tree += minEdge
                metVertices += v
                minHeap ++= vToE(v).map(_._2).filter(e => !(metVertices(e.edge.u) && metVertices(e.edge.v))  )
            }
            else if(!metVertices(u) && metVertices(v)) {
                tree = tree :+ minEdge
                metVertices += u
                minHeap ++= vToE(u).map(_._2).filter(e => !(metVertices(e.edge.u) && metVertices(e.edge.v)) )
            }
        }

        tree.toList
    }
}
