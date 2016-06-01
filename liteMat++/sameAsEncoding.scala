import org.apache.spark.graphx.Edge 
import org.apache.spark.graphx.Graph

import scala.collection.mutable.ListBuffer
import org.apache.spark.HashPartitioner

val NB_FRAGMENTS = sc.defaultParallelism
val part =  sc.defaultParallelism 

val directory = "/home/oliv/projets/lodAutomed/"
val file = "drugbank_dump.nt"

// Load the Drugbank data set
val triples0 = sc.textFile(directory+file).map(x=>x.split(" ")).map(t=>(t(0),t(1),t(2)))

// create an RDD made of the owl:sameAs statements
val sameAs = triples0.filter{case(s,p,o)=>p=="<http://www.w3.org/2002/07/owl#sameAs>"}.map{case(s,p,o)=>(s,o)}

// create an RDD containing individuals involved in sameAs triples
val sameAsInd = sameAs.flatMap{case(s,o)=>Array(s,o)}.distinct

// provide a unique Long id to all sameAs individuals
val sameAsIndId = sameAsInd.zipWithUniqueId

// create edges of the graph
val sameAsEdges = sameAs.join(sameAsIndId).map{case(s,(o,id))=>(o,id)}.join(sameAsIndId).map{case(o,(idS,idO))=>Edge(idS,idO,null)}

// create sameAs graph
val sameAsGraph = Graph(sameAsIndId.map{case(uri,id)=>(id,uri)}, sameAsEdges)

// Compute connected components of the graph
val connectedComponents = sameAsGraph.connectedComponents

// create an RDD containing the ids of connected components
val sameAsGroup = connectedComponents.vertices.map{x=> x._2}.distinct

// create an RDD made of statements where the property is not owl:sameAs
val nonSameAs = triples0.filter{case(s,p,o)=>p!="<http://www.w3.org/2002/07/owl#sameAs>"}.map{case(s,p,o)=>(s,o)}

// create an RDD containing nonSameAs individuals
val nonSameAsInd = nonSameAs.flatMap{case(s,o)=>Array(s,o)}.distinct

// number of bits required for the encoding of sameAs individuals
val nonSameAsBit = (Math.log(nonSameAsInd.count*2)/Math.log(2)).ceil

val nonSameAsDictionary = nonSameAsInd.zipWithUniqueId

// number of bits required for the encoding of sameAs individuals
val sameAsBit = 1 + nonSameAsBit + (Math.log(sameAsGroup.count*2)/Math.log(2)).ceil


