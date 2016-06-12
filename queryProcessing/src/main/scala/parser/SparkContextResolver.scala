package parser

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object SparkContextResolver {

  def apply: SparkContext = {
    val sparkConf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer", "24")
      .setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    var sc:SparkContext = new SparkContext(sparkConf)
    Logger.getRootLogger.setLevel(Level.WARN)
    sc
  }
}
