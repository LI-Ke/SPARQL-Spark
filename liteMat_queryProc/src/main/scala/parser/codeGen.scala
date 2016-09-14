import scala.collection.mutable
import org.apache.spark.sql.DataFrame

import scala.reflect.runtime.{universe => ru}
import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox


val encodedAbox = "/home/oliv/git/SPARQL-Spark/liteMat++/drugbankExt/eabox"
val directory = "/home/oliv/git/SPARQL-Spark/liteMat++/drugbankExt/"

val triples = sc.textFile(encodedAbox).map(x=>x.split(" ")).map(x=>(x(0).toLong, x(1).toLong,x(2).toLong))
val df = triples.map{case(s,p,o)=>(s,p,o)}.toDF("s","p","o")
df.persist
df.count

val sp = sc.parallelize(Array(("sas1","276","x"),("sas1","306","z")))

var dfs = new mutable.ArrayBuffer[DataFrame]
var count = 0

sp.foreach{ x=> {val pBlock = "df(p)==="+{x._2}; println(pBlock)}}

sp.foreach{ x=> {val pBlock = "df(p)=="+{x._2}; val df = q"df.where($pBlock)"}}


//============= Test with reify

import scala.reflect.runtime.{universe => u}

val expr = u reify { 1 to 3 map (_+1) }

u show expr.tree

import scala.tools.reflect.ToolBox
import scala.reflect.runtime.{currentMirror => m}
val tb = m.mkToolBox()
val tree = tb.parse("1 to 3 map (_+1)")
val eval = tb.eval(tree)
