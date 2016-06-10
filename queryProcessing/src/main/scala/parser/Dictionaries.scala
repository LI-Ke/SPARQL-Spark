package parser

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.broadcast.Broadcast

class Dictionaries(sc : SparkContext, directory : String) {

    val metadata = sc.textFile(directory+"/dct/metadata").map(x=>x.split(" ")).map(x=>(x(0),x(1)))
    val saGroupBits = metadata.lookup("saGroupBits").apply(0).toLong
    val saLocalBits = metadata.lookup("saLocalBits").apply(0).toLong

    val sameAsStartId = 1<< (saGroupBits+saLocalBits)

    val concepts = sc.textFile(directory+"/dct/concepts.dct").map(x=>x.split(" "))
    val conceptsId2URI = concepts.map(x=>(x(1),x(0),x(2)))
    conceptsId2URI.persist
    println("Count de conceptsId2URI ="+conceptsId2URI.count)
    
    val conceptsURI2Id = concepts.map(x=>(x(0),x(1)))
    conceptsURI2Id.persist
    conceptsURI2Id.count    

    val properties = sc.textFile(directory+"/dct/properties.dct").map(x=>x.split(" "))
    val propertiesId2URI = properties.map(x=>(x(1),x(0),x(2)))
    propertiesId2URI.persist
    propertiesId2URI.count

    val propertiesURI2Id = properties.map(x=>(x(0),x(1)))
    propertiesURI2Id.persist
    propertiesURI2Id.count

    val sameAs = sc.textFile(directory+"/dct/sameAs.dct").map(x=>x.split(" "))
    val sameAsId2URI = sameAs.map(x=>(x(0),x(1)))
    sameAsId2URI.persist
    sameAsId2URI.count

    val sameAsURI2Id = sameAs.map(x=>(x(1),x(0)))
    sameAsURI2Id.persist
    sameAsURI2Id.count

    val nonSameAs = sc.textFile(directory+"/dct/nonSameAs.dct").map(x=>x.split(" "))
    val nonSameAsId2URI = nonSameAs.map(x=>(x(0),x(1)))
    nonSameAsId2URI.persist
    nonSameAsId2URI.count

    val nonSameAsURI2Id = nonSameAs.map(x=>(x(1),x(0)))
    nonSameAsURI2Id.persist
    nonSameAsURI2Id.count
}

object Dictionaries {
  def apply(sc: SparkContext, directory: String): Dictionaries = new Dictionaries(sc, directory)

}
