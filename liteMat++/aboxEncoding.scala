import scala.collection.JavaConversions._
import java.io.File

val directory = "/home/oliv/git/SPARQL-Spark/liteMat++/drugbankExt/"
val file = "drugbankExt.nt"

// Load the Drugbank data set
val triples = sc.textFile(directory+file).map(x=>x.split(" ")).map(t=>(t(0),t(1),t(2)))

// load the dictionaries
val concepts = sc.textFile(directory+"/dct/concepts.dct").map(x=>x.split(" "))
val conceptsId2URI = concepts.map(x=>(x(1),x(0),x(2)))
conceptsId2URI.persist
val conceptsURI2Id = concepts.map(x=>(x(0),x(1)))
conceptsURI2Id.persist

val properties = sc.textFile(directory+"/dct/properties.dct").map(x=>x.split(" "))
val propertiesId2URI = properties.map(x=>(x(1),"<"+x(0)+">",x(2)))
conceptsId2URI.persist
val propertiesURI2Id = properties.map(x=>("<"+x(0)+">",x(1)))
conceptsURI2Id.persist

val sameAs = sc.textFile(directory+"/dct/sameAs.dct").map(x=>x.split(" "))
val sameAsId2URI = sameAs.map(x=>(x(0),x(1)))
sameAsId2URI.persist
val sameAsURI2Id = sameAs.map(x=>(x(1),x(0)))
sameAsURI2Id.persist

val nonSameAs = sc.textFile(directory+"/dct/nonSameAs.dct").map(x=>x.split(" "))
val nonSameAsId2URI = nonSameAs.map(x=>(x(0),x(1)))
nonSameAsId2URI.persist
val nonSameAsURI2Id = nonSameAs.map(x=>(x(1),x(0)))
nonSameAsURI2Id.persist

/////////////////////////////
// Encode Abox dataset

// remove sameAs statements
val abox0 = triples.filter(x=>x._2!="<http://www.w3.org/2002/07/owl#sameAs>").map(x=>(x._2,(x._1,x._3)))

// encode property elements
val abox1 = abox0.join(propertiesURI2Id).map{case(p,((s,o),idp))=>(s,(idp,o))}

// encode subject elements
val abox2 = abox1.join(sameAsURI2Id).union(abox1.join(nonSameAsURI2Id)).map{case(s,((p,o),ids))=>(o,(ids,p))}

// encode property elements
val abox = abox2.join(sameAsURI2Id).union(abox2.join(nonSameAsURI2Id)).union(abox2.join(conceptsURI2Id)).map{case(o,((s,p),ido))=>(s,p,ido)}

abox.map(x=>x._1+" "+x._2+" "+x._3).saveAsTextFile(directory+"/eabox/")
