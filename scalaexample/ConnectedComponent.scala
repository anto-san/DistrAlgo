import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.collection.mutable.ListBuffer



object BFS {

	def main(args: Array[String]) {

		// set Spark configuration and context
		val conf = new SparkConf()
		conf.setAppName("SimpleName")
		conf.setMaster("local[*]")
		val sc = new SparkContext(conf)
		Logger.getLogger("org").setLevel(Level.WARN)
		Logger.getLogger("akka").setLevel(Level.WARN)

		// record starting time and define logger
		val mainTime = System.nanoTime()
		val log = Logger.getLogger(getClass.getName)

		// define a graph and cache it
		val vertexArray = Array(
			(1L, ("Alice", "Kensington", 28)),
			(2L, ("Bob", "Kensington", 27)),
			(3L, ("Charlie", "Hebdo", 65)),
			(4L, ("David", "Hebdo", 42)),
			(5L, ("Ed", "Guerrero", 55)),
			(6L, ("Fran", "Souvignon", 50)),
			(7L, ("George","Glasgow", 60)),
			(8L, ("Holly", "Edinburgh", 22)),
			(9L, ("Ivanka", "London", 40)),
			(10L, ("Jon", "Snow", 26)),
			(11L, ("Sansa", "Stark", 20))
		)
		val edgeArray = Array(
			Edge(2L, 1L, 7),
			Edge(2L, 4L, 2),
			Edge(3L, 2L, 4),
			Edge(3L, 6L, 3),
			Edge(4L, 1L, 1),
			Edge(5L, 2L, 2),
			Edge(5L, 3L, 8),
			Edge(5L, 6L, 3),
			//Edge(7L, 5L, 4),
			//second graph
			Edge(8L, 7L, 2),
			Edge(9L, 8L, 3),
		//	Edge(9L, 10L, 2),
			Edge(10L, 11L, 3)
		)
		val vertexRDD: RDD[(Long, (String, String, Int))] = sc.parallelize(vertexArray)
		val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)
		val graph: Graph[(String, String, Int), Int] = Graph(vertexRDD, edgeRDD)
		/*val graph = GraphLoader.edgeListFile(sc,"p2p-Gnutella08.txt")*/
		graph.cache()

		// define the Root vertex from which start BFS and initialize vertices' markings as visited or unvisited
		val rootVertex = graph.vertices.first()._1
		val initialGraph = graph.mapVertices((id, attr) => if (id == rootVertex) 0.0 else Double.PositiveInfinity)

		// Unpersisting the previous graph and caching newly generated graph
		graph.unpersist(blocking = false)
		initialGraph.cache()

		// Pregel stuff
		// EdgeDirection.Either means that messages will be sent on the edge if any vertex of the triplet received some message
		val initialMessage = Double.PositiveInfinity
		val maxIterations = Int.MaxValue
		val activeEdgeDirection = EdgeDirection.Either

		log.info("Result Connected Component")

		val connectedComponent = graph.connectedComponents().vertices
	//	connectedComponent.collect.foreach(n =>println(n))
		val groupedComponents = connectedComponent.groupBy({ case (vertexId,ccId)=>ccId})
		println()
		println("Connected component count is: " + groupedComponents.count())
		println()
		println("Connected components are:")
		println()
		groupedComponents.collect.foreach(println)
		println()

	}


}


