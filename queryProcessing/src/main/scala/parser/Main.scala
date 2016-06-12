package parser

object Main extends App {
  println("start")
  //val qr = "SELECT ?x ?z1 WHERE {<http://www.dbpedia.org/resource/Clotrimazole> <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/brandName> ?x. ?x <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/brandName> ?y. ?x <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/brandName> ?z. ?x <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/brandName> ?z1. ?y <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/brandName> ?z. ?y <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/brandName> ?z2. ?y <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/brandName> ?z3.}"
  val qr = "SELECT ?x ?z WHERE {<http://www.dbpedia.org/resource/Clotrimazole> <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/brandName> ?x. <http://www.dbpedia.org/resource/Clotrimazole> <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/atcCode> \"D01AC01\". ?z <http://www.w3.org/2002/07/owl#sameAs> <http://www.dbpedia.org/resource/Clotrimazole>}"

  val directory = "/home/oliv/git/SPARQL-Spark/liteMat++/drugbankExt/"

  val sc = SparkContextResolver.apply
  val visitor = SparqlQueryResolver(qr).visitor

  val dictionaries = Dictionaries(sc, directory)

  val collector = SparqlOpCollector(visitor, dictionaries)

  val lp = OptimizedPlan(collector).createLogicalPlan

//  val pp = QueryPhysicalPlan(lp)
  println("end")
}
