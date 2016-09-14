package parser
import org.openrdf.query.algebra.{ProjectionElem, StatementPattern, Var}

import scala.collection.mutable

class SparqlOpCollector(visitor: SparqlOpVisitor, dictionaries : Dictionaries) {

  val opMap = new mutable.LinkedHashMap[StatementPattern, String]
  val opProjects = new mutable.ArrayBuffer[ProjectionElem]
  val opSPMap = new mutable.LinkedHashMap[StatementPattern, Int]

  val predicates = new mutable.ArrayBuffer[Var]
  val varWeightMap = new mutable.LinkedHashMap[String, Int]
  var sameAsMap = new mutable.HashMap[Long,(Var, Long, Long)]
  val sameAsVarPrefix = "sAs"
  var sameAsVarCount = 1

  private var opId: Int = 0

  visitor.projects.foreach(x => {
    println("var = "+x);initOpProjects(x); projectsWeight(x)
  })
  visitor.statementPatterns.foreach(x => {
    val idP = processPredicate(x); processObject(x,idP); processSubject(x); initOpSPsMap(x); spsWeight(x); println(x.toString)
  })
  sameAsMap.foreach(println)
  varWeightMap.retain((key, value) => value >= 2)


  def initOpMap() = {

  }

  def initOpProjects(project: ProjectionElem) = {
    opProjects.append(project)
  }

  def projectsWeight(project: ProjectionElem) = {
    varWeightMap.put(project.getTargetName, 1)
  }

  def processPredicate(sp: StatementPattern) = {
    var idP = -1L
    if (sp.getPredicateVar.hasValue) {
        idP =   dictionaries.propertiesURI2Id.lookup(sp.getPredicateVar.getValue.toString).apply(0).toLong
        if (idP>=0) {
	   sp.setPredicateVar(new Var(idP.toString))
        } 
    }
    predicates.append(sp.getPredicateVar)
    idP
  }
  def processObject(sp: StatementPattern, idP : Long) = {
    if(sp.getObjectVar.hasValue) {
	var obj = sp.getObjectVar.getValue.stringValue
        if(obj.trim.startsWith("http"))
            obj = "<"+obj+">"
	val idO = idP match {
	    case 0 => dictionaries.conceptsURI2Id.lookup(obj)
	    case _ => dictionaries.sameAsURI2Id.union(dictionaries.nonSameAsURI2Id).lookup(objsma)
	}
	sp.setObjectVar(getVariable(idO.apply(0).toLong))
    }
  }

  def processSubject(sp: StatementPattern) = {
    if(sp.getSubjectVar.hasValue) {
        var subject = sp.getSubjectVar.getValue.stringValue
        if(subject.trim.startsWith("http"))
            subject = "<"+subject+">"
	val idS = dictionaries.sameAsURI2Id.union(dictionaries.nonSameAsURI2Id).lookup(subject).apply(0).toLong
        sp.setSubjectVar(getVariable(idS))
    }
  }

  def getVariable(id: Long) : Var = {
      if(id>=dictionaries.sameAsStartId) {
          if(sameAsMap.contains(id)) {
	      sameAsMap.apply(id)._1
          }
	  else {
 	      val boundBase = id >> dictionaries.saLocalBits
              val lowerBound = boundBase << dictionaries.saLocalBits
              val upperBound = (boundBase +1) << dictionaries.saLocalBits
              val tmpVar = new Var(sameAsVarPrefix+""+sameAsVarCount)
              sameAsMap += (id -> (tmpVar,lowerBound.toLong, upperBound.toLong))
              sameAsVarCount = sameAsVarCount + 1
              tmpVar
	  }
      }
      else
          new Var(id.toString)
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
