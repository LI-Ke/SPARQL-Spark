import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD

val nodes = sc.parallelize(Array((1L,"A"),(2L,"B"),(3L,"C"),(4L,"D"),(5L,"E"),(6L,"F"),(7L,"G"),(8L,"H"),(9L,"I")))
val edges = sc.parallelize(Array((1,2,"p"),(2,3,"p"),(4,5,"p"),(5,6,"p"),(6,7,"p"),(7,8,"p"),(8,9,"p"),(1,4,"q"),(2,5,"q"),(3,6,"q")))

val pEdges = edges.filter(x=>x._3=="p").map(x=>Edge(x._1.toLong,x._2.toLong,null))
val graph = Graph(nodes, pEdges)
val cc = graph.connectedComponents
val idCC = cc.vertices.map{x=> x._2.toLong}.distinct.sortBy(x=>x,true)
// Is there a path from 5 to 7
val s=5
val o=8
val can =idCC.filter(x=>x<s)

val comp = can.sortBy(x=>x,false).first

val r = cc.vertices.filter(x=>x._2==comp.toLong && x._1==o.toLong)

if(r.count >0)   println("candidate") else   println("5 and 7 not related by p")


// compute transitive closure


var edgesT = cc.edges.filter(x=>x.srcId==s).map(x=>(x.dstId,x.srcId))
val edges2 = cc.edges.map(x=>(x.srcId,x.dstId))
var newCount = edgesT.count
var oldCount = newCount
var continue = true
do {
   oldCount = newCount
   edgesT = edgesT.union(edgesT.join(edges2).map(x => (x._2._2, x._1))).distinct().cache()
   if(edgesT.filter(x=>x._1==o).count>0)
     continue = false
   newCount = edgesT.count()
} while (newCount != oldCount && continue)

// This version works for s and o fixed. To do: s or o is a variable


