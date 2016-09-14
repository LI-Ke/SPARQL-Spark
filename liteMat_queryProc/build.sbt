import AssemblyKeys._

assemblySettings

mergeStrategy in assembly := {
  case x if x.startsWith("META-INF") => MergeStrategy.discard // Bumf
  case x if x.endsWith(".html") => MergeStrategy.discard // More bumf
  case x if x.contains("slf4j-api") => MergeStrategy.last
  case x if x.contains(".class") => MergeStrategy.last
  case x if x.contains("org/cyberneko/html") => MergeStrategy.first
  case PathList("com", "esotericsoftware", xs@_ *) => MergeStrategy.last // For Log$Logger.class
  case x =>
     val oldStrategy = (mergeStrategy in assembly).value
     oldStrategy(x)
}

name := "queryProcessor"

version := "0.1.0"

scalaVersion := "2.10.4"

//Spark dependencies
libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.5.2"%"provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.5.2"
libraryDependencies += "org.apache.spark" % "spark-graphx_2.10" % "1.5.2"

//sesame dependencies
libraryDependencies += "org.openrdf.sesame" % "sesame-model" % "4.1.2"
libraryDependencies += "org.openrdf.sesame" % "sesame-query" % "4.1.2"
libraryDependencies += "org.openrdf.sesame" % "sesame-queryparser-sparql" % "4.1.2"
