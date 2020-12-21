import mstproject.Boruvka
import mstproject.Model._
import org.scalatest.funsuite.AnyFunSuite

class BoruvkaTest extends AnyFunSuite {
  val sampleEdges = List(((1,4),14), ((3,5),5), ((4,6),6), ((1,2),7), ((2,5),3), ((2,3),8), ((5,6),10), ((2,4),11), ((5,7),12), ((6,7),13), ((4,5),15))
  val edges: List[weightedEdge] = sampleEdges.map(r => weightedEdge( myEdge(r._1._1, r._1._2), r._2))
  val boruvka: List[weightedEdge] = Boruvka(edges)
  val correctResult = List(weightedEdge(myEdge(2,5),3), weightedEdge(myEdge(3,5),5), weightedEdge(myEdge(4,6),6), weightedEdge(myEdge(1,2),7), weightedEdge(myEdge(5,6),10), weightedEdge(myEdge(5,7),12))// :+ weightedEdge(myEdge(4,5),15)
  test("Boruvka must return correct result for sampleEdges"){
    assert(boruvka.length == 6)

    correctResult.foreach { e =>
      assert(boruvka.contains(e))
    }
  }

  val sampleEdges2: List[weightedEdge] = List(((6,1),6), ((6,7),13), ((1,7),9), ((1,3),4), ((7,2),5), ((5,7), 11), ((2,5),8), ((3,7),7), ((4,7),2), ((3,4),6), ((3,5),10)).map(r => weightedEdge( myEdge(r._1._1, r._1._2), r._2))
  val boruvka2: List[weightedEdge] = Boruvka(sampleEdges2)
  val correctResult2: List[weightedEdge] = List(((6,1),6), ((1,3),4), ((7,2),5), ((2,5),8), ((4,7),2), ((3,4),6)).map(r => weightedEdge( myEdge(r._1._1, r._1._2), r._2))
  test("Boruvka must return correct result for sampleEdges2"){
    assert(boruvka2.length == 6)
    assert(boruvka2.map(_.weight).sum == 31)

    correctResult2.foreach { e =>
      assert(boruvka2.contains(e))
    }
  }

  // https://en.wikipedia.org/wiki/File:Boruvka%27s_algorithm_(Sollin%27s_algorithm)_Anim.gif
  val sampleEdges3: List[weightedEdge] = List(((1,2),13), ((1,3),6), ((3,2),7), ((2,4),1), ((3,5),8), ((3,4),14), ((4,5),9), ((4,6),3), ((5,6),2), ((3,8),20), ((5,10),18), ((7,8),15), ((7,9),5), ((8,10),17), ((9,11),11), ((11,10),16), ((10,12),4), ((11,12),12), ((10,7),19), ((11,7),10)).map(r => weightedEdge( myEdge(r._1._1, r._1._2), r._2))
  val boruvka3: List[weightedEdge] = Boruvka(sampleEdges3)
  val correctResult3: List[weightedEdge] = List(((1,3),6), ((2,4),1), ((3,2),7), ((4,6),3), ((5,6),2), ((7,8),15), ((7,9),5), ((10,12),4), ((11,12),12), ((11,7),10), ((5,10),18)).map(r => weightedEdge( myEdge(r._1._1, r._1._2), r._2))
  test("Boruvka must return correct result for sampleEdges3 the 'wikipedia sample graph'"){
    assert(boruvka3.length == 11)
    assert(boruvka3.map(_.weight).sum == correctResult3.map(_.weight).sum)

    correctResult3.foreach { e =>
      assert(boruvka3.contains(e))
    }
  }

  val sampleEdges4: List[weightedEdge] = List(((1,3),6), ((2,3),7), ((4,3),5)).map(r => weightedEdge( myEdge(r._1._1, r._1._2), r._2))
  val boruvka4: List[weightedEdge] = Boruvka(sampleEdges4)
  val correctResult4: List[weightedEdge] = List(((1,3),6), ((2,3),7), ((4,3),5)).map(r => weightedEdge( myEdge(r._1._1, r._1._2), r._2))
  test("Boruvka must return correct result for sampleEdges4 the 'Tree'"){
    assert(boruvka4.length == correctResult4.length)
    assert(boruvka4.map(_.weight).sum == correctResult4.map(_.weight).sum)

    correctResult4.foreach { e =>
      assert(boruvka4.contains(e))
    }
  }
}

//Note replace 'test' with 'ignore' to skip the test
//  test("Invoking head on an empty Set should produce NoSuchElementException") {
//    assertThrows[NoSuchElementException] {
//      Set.empty.head
//    }
//  }
