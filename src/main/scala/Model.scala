package mstproject

object Model {
  case class myEdge(u: Int, v: Int)
  case class weightedEdge(edge: myEdge, weight: Int)
}
