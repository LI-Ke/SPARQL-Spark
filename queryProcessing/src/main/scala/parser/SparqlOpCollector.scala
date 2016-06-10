package parser
import org.openrdf.query.algebra.{ProjectionElem, StatementPattern, Var}

import scala.collection.mutable

/**
  * Created by xiangnanren on 17/05/16.
  */
class SparqlOpCollector(visitor: SparqlOpVisitor, dictionaries : Dictionaries) {

  val opMap = new mutable.LinkedHashMap[StatementPattern, String]
  val opProjects = new mutable.ArrayBuffer[ProjectionElem]
  val opSPMap = new mutable.LinkedHashMap[StatementPattern, Int]

  val predicates = new mutable.ArrayBuffer[Var]
  val varWeightMap = new mutable.LinkedHashMap[String, Int]

  private var opId: Int = 0

  visitor.projects.foreach(x => {
    println("var = "+x);initOpProjects(x); projectsWeight(x)
  })
  visitor.statementPatterns.foreach(x => {
    initPredicates(x); initOpSPsMap(x); spsWeight(x)
  })
  varWeightMap.retain((key, value) => value >= 2)


  def initOpMap() = {

  }

  def initOpProjects(project: ProjectionElem) = {
    opProjects.append(project)
  }

  def projectsWeight(project: ProjectionElem) = {
    varWeightMap.put(project.getTargetName, 1)
  }

  def initPredicates(sp: StatementPattern) = {
    var idP = -1L
    if (sp.getPredicateVar.hasValue) {
        idP =   dictionaries.propertiesURI2Id.lookup(sp.getPredicateVar.getValue.toString).apply(0).toLong
        if (idP>=0) {
	   sp.setPredicateVar(new Var(idP.toString))
        }
    }
    if(sp.getObjectVar.hasValue) {
	val idO = idP match {
	    case 0 => dictionaries.conceptsURI2Id.lookup(sp.getObjectVar.getValue.stringValue)
	    case _ => dictionaries.sameAsURI2Id.union(dictionaries.nonSameAsURI2Id).lookup(sp.getObjectVar.getValue.stringValue)
	}
	sp.setObjectVar(new Var(idO.apply(0).toString))
    }
    if(sp.getSubjectVar.hasValue) {
        var subject = sp.getSubjectVar.getValue.stringValue
        if(subject.trim.startsWith("http"))
          subject = "<"+subject+">"
           
	val idS = dictionaries.sameAsURI2Id.union(dictionaries.nonSameAsURI2Id).lookup(subject).apply(0).toString

	sp.setSubjectVar(new Var(idS))
    }
    predicates.append(sp.getPredicateVar)

    println("New statement = "+sp.toString)
    if(idS>=dictionaries.sameAsStartId) {
       val boundBase = idS >> dictionaries.saLocalBits
       val lowerBound = boundBase << dictionaries.saLocalBits
       val upperBound = (boundBase +1) << dictionaries.saLocalBits
       println(s"Subject becomes a variable and is associated to a filter : FILTER (?var>=$lowerBound && ?var<$upperBound)")
    }
  }

  def initOpSPsMap(sp: StatementPattern) = {
    opSPMap.put(sp, opId)
    opId += 1
  }

  def spsWeight(sp: StatementPattern) = {
    if (!sp.getSubjectVar.hasValue) {
      if (!varWeightMap.contains(sp.getSubjectVar.getName)) varWeightMap.put(sp.getSubjectVar.getName, 0)
      varWeightMap(sp.getSubjectVar.getName) += 1
    }

    if (!sp.getPredicateVar.hasValue) {
      if (!varWeightMap.contains(sp.getPredicateVar.getName)) varWeightMap.put(sp.getPredicateVar.getName, 0)
      varWeightMap(sp.getPredicateVar.getName) += 1
    }

    if (!sp.getObjectVar.hasValue) {
      if (!varWeightMap.contains(sp.getObjectVar.getName)) varWeightMap.put(sp.getObjectVar.getName, 0)
      varWeightMap(sp.getObjectVar.getName) += 1
    }
  }
}

object SparqlOpCollector {
  def apply(visitor: SparqlOpVisitor, dictionaries : Dictionaries): SparqlOpCollector = new SparqlOpCollector(visitor, dictionaries)

}
