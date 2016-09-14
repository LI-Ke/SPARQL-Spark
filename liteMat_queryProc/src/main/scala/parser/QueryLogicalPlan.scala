package parser

abstract class QueryLogicalPlan(collector: SparqlOpCollector) {

  def createLogicalPlan: String

}
