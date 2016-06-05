# SPARQL-Spark
The goal of this project is to create a RDF Store that runs over Spark.
This includes :
- the distribution of the RDF data set
- SPARQL query processing (using Spark SQL) 
- inference over RDFS, RDFS++ and we hope more expressive ontology languages:
   - using the LiteMat encoding scheme
       - specific Abox encoding in LiteMat++ with:
	 - sameAsEncoding.scala to provide identifiers for sameAs and nonSameAs individuals
	 - aboxEncoding.scala to encode the Abox data set.
         - queryProcessing.scala to give the principles of query processing within LiteMat encoding.
 
