package mstproject

/** corresponds custom data models definition */
object Model {
  case class myEdge(u: Int, v: Int)
  case class weightedEdge(edge: myEdge, weight: Int, removed: Boolean = false, selectedInStep: Int = -1, selectedByV: Int = -1, id: Int)

  case class vertexEdge(v: Int, edge: weightedEdge)
  case class vEdges(v: Int, edges: Iterator[weightedEdge])
}
