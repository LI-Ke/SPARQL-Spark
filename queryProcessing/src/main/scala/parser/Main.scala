package parser

object Main extends App {
  println("start")
  val qr = "SELECT ?x WHERE {<http://www.dbpedia.org/resource/Clotrimazole> <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/brandName> ?x .}"
  val visitor = SparqlQueryResolver(qr).visitor

  val collector = SparqlOpCollector(visitor)

  val lp = OptimizedPlan(collector).createLogicalPlan

  val pp = QueryPhysicalPlan(lp)
  println("end")
}
