// imports
//import org.apache.spark.SparkContext
//import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

// main object
object Simple {
  def main(args: Array[String]) {

	// set Spark configuration and context (for telling Spark how to access the cluster - master not necessary if running from terminal)
	val conf = new SparkConf()
	conf.setAppName("SimpleName")
	val sc = new SparkContext(conf)

	// to define a property graph, first it is necessary to define vertices and edges
	// - vertices are composed by a unique long ID and a property tuple (name, surname and age in this case)
	// - edges are given by source vertex ID, destination vertex ID and an attribute over the edge
	val vertexArray = Array(
	  (1L, ("Alice", "Kensington", 28)),
	  (2L, ("Bob", "Kensington", 27)),
	  (3L, ("Charlie", "Hebdo", 65)),
	  (4L, ("David", "Hebdo", 42)),
	  (5L, ("Ed", "Guerrero", 55)),
	  (6L, ("Fran", "Souvignon", 50))
	  )
	val edgeArray = Array(
	  Edge(2L, 1L, 7),
	  Edge(2L, 4L, 2),
	  Edge(3L, 2L, 4),
	  Edge(3L, 6L, 3),
	  Edge(4L, 1L, 1),
	  Edge(5L, 2L, 2),
	  Edge(5L, 3L, 8),
	  Edge(5L, 6L, 3)
	  )	
	val vertexRDD: RDD[(Long, (String, String, Int))] = sc.parallelize(vertexArray)
	val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)
	val graph: Graph[(String, String, Int), Int] = Graph(vertexRDD, edgeRDD)

	// once the graph is built, it is possible to do various operations over its vertices and edges
	val vertices = graph.vertices

	// count all vertices
	val count = vertices.count()
	println("The graph has " + count + " vertices")

	// collect (retrieve from parallelization) the vertices and print them
	var collect_all = vertices.collect()
	println("The graph vertices are:")
	collect_all.foreach(vertex => println(vertex))

	// print even the names (every vertex is a tuple of ID and properties, that are tuples as well)
	println("Where the first names are:")
	collect_all.foreach(vertex => println(vertex._2._1))

	// now collect only some vertices and print their name (in two different compact ways)
	vertices.filter(vertex => vertex._2._3 > 30).collect.foreach(v => println(s"${v._2._1} is the name"))
	vertices.filter{case(id,(name,surname,age)) => age > 30}
		.collect.foreach{case(id,(name,surname,age)) => println(s"$name $surname is $age years old")}

	// in addition, there are triplets for every pair of vertices, combining data of vertices and edges (in two ways)
	val triplets = graph.triplets
	triplets.foreach(t => println(s"There is an edge from ${t.srcAttr._1} to ${t.dstAttr._1} with value ${t.attr}"))
	for (t <- triplets.collect) {
		println(s"There is an edge from ${t.srcAttr._1} to ${t.dstAttr._1} with value ${t.attr}")
	}

	// find vertices with attr value greater than 5 over the edge that connects them (the same can be done with foreach)
	for (t <- triplets.filter(triplet => triplet.attr>5).collect) {
		println(s"There is an edge from ${t.srcAttr._1} to ${t.dstAttr._1} with value ${t.attr}")
	}		
	
	// create a new version of an existing graph, modifying the properties of vertices (initialize, add degrees, print)
	// - when using vertices mappings, just return modified properties without vertexID
	// - when using outerJoinVertices, pass the vertices with the property to add and then use a matching option to fuse values
	val newGraph = graph.mapVertices{case(id,(name,surname,age)) => (name,surname,age,0,0)}
	val newGraph2 = newGraph.outerJoinVertices(graph.inDegrees)	{case(id,(name,surname,age,in,out),inOpt) => 
									(name,surname,age,inOpt.getOrElse(0),out)}
	val newGraph3 = newGraph2.outerJoinVertices(graph.outDegrees)	{case(id,(name,surname,age,in,out),outOpt) => 
									(name,surname,age,in,outOpt.getOrElse(0))}
	newGraph3.vertices.collect().foreach(vertex => println(vertex))

	// it is possible to aggregate information about the neighborhood of a vertex, using aggregateMessages function
	// (it is essentially a map reduce approach on the triplets of a vertex)

	// TO READ A GRAPH FROM A TEXT FILE
	val graph2 = GraphLoader.edgeListFile(sc,"p2p-Gnutella08.txt")
	val cont = graph2.vertices.count()
	println(s"The graph load from the text file has $cont vertices")

  }
}
