import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.collection.mutable.ListBuffer



object ConnectedComponent {

	def main(args: Array[String]) {

		// set Spark configuration and context
		val conf = new SparkConf()
		conf.setAppName("SimpleName")
		
		/*This function you use only for test on your pc*/
		//conf.setMaster("local[*]")
		val sc = new SparkContext(conf)
		Logger.getLogger("org").setLevel(Level.WARN)
		Logger.getLogger("akka").setLevel(Level.WARN)

		// record starting time and define logger
		val mainTime = System.nanoTime()
		val log = Logger.getLogger(getClass.getName)
		def calcTime(s1:Long, s2:Long): String = { 
		BigDecimal((s1-s2)/1e6).setScale(2, BigDecimal.RoundingMode.HALF_UP).toString}
	
		val graph = GraphLoader.edgeListFile(sc, args(1))
		graph.cache()

		// define the Root vertex from which start BFS and initialize vertices' markings as visited or unvisited
		val rootVertex = graph.vertices.first()._1
		val initialGraph = graph.mapVertices((id, attr) => if (id == rootVertex) 0.0 else Double.PositiveInfinity)
		val timeStart = System.nanoTime()
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

		val computTime = calcTime(System.nanoTime, timeStart)
		println("CC execution time: "+ computTime + " milliseconds")
		val groupedComponents = connectedComponent.groupBy({ case (vertexId,ccId)=>ccId})
		println()
		println("Connected component count is: " + groupedComponents.count())
		println()
		println()
		

	}


}


