val encodedAbox = "/home/oliv/git/SPARQL-Spark/liteMat++/drugbankExt/eabox"
val directory = "/home/oliv/git/SPARQL-Spark/liteMat++/drugbankExt/"

val triples = sc.textFile(encodedAbox).map(x=>x.split(" ")).map(x=>(x(0).toLong, x(1).toLong,x(2).toLong))
val df = triples.map{case(s,p,o)=>(s,p,o)}.toDF("s","p","o")
df.persist
df.count

val sameAs = sc.textFile(directory+"/dct/sameAs.dct").map(x=>x.split(" "))
val sameAsId2URI = sameAs.map(x=>(x(0).toLong,x(1))).toDF("s","sLib")
sameAsId2URI.persist
val sameAsURI2Id = sameAs.map(x=>(x(1),x(0)))
sameAsURI2Id.persist

val nonSameAs = sc.textFile(directory+"/dct/nonSameAs.dct").map(x=>x.split(" "))
val nonSameAsId2URI = nonSameAs.map(x=>(x(0).toLong,x(1))).toDF("o","oLib")
nonSameAsId2URI.persist
val nonSameAsURI2Id = nonSameAs.map(x=>(x(1),x(0)))
nonSameAsURI2Id.persist

//////////////////////////////////
// Query 1 : ASK {<http://www.dbpedia.org/resource/Clotrimazole> <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/brandName> "Veltrim" .}
// translated into ASK {?sa1 276 3. FILTER (?sa1>=160 && ?sa1<192}

var start= java.lang.System.currentTimeMillis();
val resQ1 = df.where(df("p")===276 && df("o")===3 && df("s")>=160 && df("s")<192)
if(resQ1.count>0) println("True") else  println("False")

var end= java.lang.System.currentTimeMillis();
print("Duration Q1 ="+(end-start))

//////////////////////////////////
// Query 2 : SELECT ?x WHERE {<http://www.dbpedia.org/resource/Clotrimazole> <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/brandName> ?y .}
var start= java.lang.System.currentTimeMillis();
val resQ2 = df.where(df("p")===276 && df("s")>=160 && df("s")<192).select("o")

// We can detect whether the result is in sameAs or nonSameAs based on the answer value and the limit of sameAs values (to be stored in a metadata of the dataset)
val ansQ2 = resQ2.join(nonSameAsId2URI,Seq("o")).select("oLib")
ansQ2.collect
var end= java.lang.System.currentTimeMillis();
print("Duration Q2 ="+(end-start))

//////////////////////////////////
// Query 3 : SELECT ?x ?y WHERE {?x <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/brandName> ?x .}
var start= java.lang.System.currentTimeMillis();
val resQ3 = df.where(df("p")===276).select("s","o")

// We can detect whether the result is in sameAs or nonSameAs based on the answer value and the limit of sameAs values (to be stored in a metadata of the dataset)
val ansQ3 = resQ3.join(nonSameAsId2URI,Seq("o")).join(sameAsId2URI,Seq("s")).select("sLib","oLib")
ansQ3.collect
var end= java.lang.System.currentTimeMillis();
print("Duration Q3 ="+(end-start))


