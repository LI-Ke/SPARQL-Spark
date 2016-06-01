import org.apache.spark.graphx.Edge 
import org.apache.spark.graphx.Graph

import scala.collection.mutable.ListBuffer
import org.apache.spark.HashPartitioner

val NB_FRAGMENTS = sc.defaultParallelism
val part =  sc.defaultParallelism 

val triples0 = sc.textFile("/home/oliv/projets/lodAutomed/drugbank_dump.nt").map(x=>x.split(" ")).map(t=>(t(0),t(1),t(2)))
val sameAs = triples0.filter{case(s,p,o)=>p=="<http://www.w3.org/2002/07/owl#sameAs>"}.map{case(s,p,o)=>(s,o)}
