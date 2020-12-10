import mstproject.Boruvka
import mstproject.Model._
import org.scalatest.funsuite.AnyFunSuite

class BoruvkaTest extends AnyFunSuite {
  val sampleEdges = List(((1,4),14), ((3,5),5), ((4,6),6), ((1,2),7), ((2,5),3), ((2,3),8), ((5,6),10), ((2,4),11), ((5,7),12), ((6,7),13), ((4,5),15))
  val edges: List[weightedEdge] = sampleEdges.map(r => weightedEdge( myEdge(r._1._1, r._1._2), r._2))
  val boruvka: List[weightedEdge] = Boruvka(edges)
  val correctResult = List(weightedEdge(myEdge(2,5),3), weightedEdge(myEdge(3,5),5), weightedEdge(myEdge(4,6),6), weightedEdge(myEdge(1,2),7), weightedEdge(myEdge(5,6),10), weightedEdge(myEdge(5,7),12))// :+ weightedEdge(myEdge(4,5),15)

  test("Boruvka must return correct number of edges for sampleEdges"){
    assert(boruvka.length == 6)
  }
  test("Boruvka must return all the edges in correctResult for sampleEdges"){
    correctResult.foreach { e =>
      assert(boruvka.contains(e))
    }
  }

//Note replace 'test' with 'ignore' to skip the test
//  test("Invoking head on an empty Set should produce NoSuchElementException") {
//    assertThrows[NoSuchElementException] {
//      Set.empty.head
//    }
//  }
}
