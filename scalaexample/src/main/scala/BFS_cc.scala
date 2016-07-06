import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger

object BFS {

	/* utility to compute the difference between two timestamps in milliseconds */
	def calcTime(s1:Long, s2:Long): String = { 
		BigDecimal((s1-s2)/1e6).setScale(2, BigDecimal.RoundingMode.HALF_UP).toString 
	}

	/* PREGEL - function applied to multiple incoming messages arriving to the same vertex, used before applying vprog */
	val reduceMessage = { (a: Double, b: Double) => math.min(a,b) }

	/* PREGEL - function used to compute a new vertex value, applied after the merging step of reduceMessage. */
	/* 	    attr is the value already stored in the vertex, msg is the inbound message (the result of reduceMessage?)*/
   	val vprog = { (id: VertexId, attr: Double, msg: Double) => math.min(attr,msg) }

	/* PREGEL - function used to create messages to send at supersteps end, for the edges in the wanted direction. */
	/* 	    In this case, if just one of the vertices is visited, send a message to the other vertex; otherwise if both are 
		    visited or not, do nothing.  */
	/* 	    It returns the iterator of a tuple (ReceiverVertexID, message) to send a message */
   	val sendMessage = { (triplet: EdgeTriplet[Double, Int]) =>
		var iter:Iterator[(VertexId, Double)] = Iterator.empty
		val isSrcMarked = triplet.srcAttr != Double.PositiveInfinity
		val isDstMarked = triplet.dstAttr != Double.PositiveInfinity

		if(!(isSrcMarked && isDstMarked)){
		   	if(isSrcMarked){				
				// prepare a new iterator only if the sent attribute is not infinite (to prevent useless infinity messages)
				if(triplet.srcAttr != Double.PositiveInfinity){
					iter = Iterator((triplet.dstId,triplet.srcAttr+1))
					println(triplet.srcId + " sending to " + triplet.dstId + " the value " + (triplet.srcAttr+1))
				}
	  		}else{
				if(triplet.dstAttr != Double.PositiveInfinity){
					iter = Iterator((triplet.srcId,triplet.dstAttr+1))
					println(triplet.dstId + " sending to " + triplet.srcId + " the value " + (triplet.dstAttr+1))
				}
	   		}
		}
		iter
   	}   

	def main(args: Array[String]) {

		// set Spark configuration and context
		val conf = new SparkConf()
		conf.setAppName("SimpleName")
		conf.setMaster("local[*]")
		val sc = new SparkContext(conf)

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
		  Edge(8L, 7L, 2),
		  Edge(9L, 8L, 3),      
		  //Edge(9L, 10L, 2),
		  Edge(10L, 11L, 3)
		  )	
		val vertexRDD: RDD[(Long, (String, String, Int))] = sc.parallelize(vertexArray)
		val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)
		val graph: Graph[(String, String, Int), Int] = Graph(vertexRDD, edgeRDD)
		/*val graph = GraphLoader.edgeListFile(sc,"p2p-Gnutella08.txt")*/
		graph.cache()
		log.info("Graph loading time: " + calcTime(System.nanoTime, mainTime) + " milliseconds.")

		// define the Root vertex from which start BFS and initialize vertices' markings as visited or unvisited
		// (notice that, using mapvertices, the attr of every vertex is mapped to a new value depending on the id)
		val rootVertex = graph.vertices.first()._1
    		val initialGraph = graph.mapVertices((id, attr) => if (id == rootVertex) 0.0 else Double.PositiveInfinity)

    		// Unpersisting the previous graph and caching newly generated graph
	    	graph.unpersist(blocking = false)		
		initialGraph.cache()

		// Pregel stuff 
		// EdgeDirection.Either means that messages will be sent on the edge if any vertex of the triplet received some message
		println("FIRST PREGEL RUN OUTPUT")
		val initialMessage = Double.PositiveInfinity
		val maxIterations = Int.MaxValue
		val activeEdgeDirection = EdgeDirection.Either
		val bfsStart = System.nanoTime()
		var bfs = initialGraph.pregel(initialMessage, maxIterations, activeEdgeDirection)(vprog, sendMessage, reduceMessage)	
		println()

		initialGraph.unpersist(blocking = false)
		bfs.cache()		

		// define if there are unvisited nodes and in the case prepare further computation
		var unvisitedVertexes = bfs.vertices.filter(vertex => vertex._2 == Double.PositiveInfinity)
		var unvisitedVertexesCount = unvisitedVertexes.count()
		var connectedCounter = 1

		// if there are unvisited nodes, it means that computation is not over... there are different connected components!
		while(unvisitedVertexesCount>0){
			println("Graph status:")
			bfs.vertices.collect.foreach(v => println(v))

			val newRoot = unvisitedVertexes.first()._1

			println()
			println("There are " + unvisitedVertexes.count() + " unvisited nodes.")
			println("The new BFS root is " + newRoot)
			println()

			println("NEW PREGEL RUN:")
			val newGraph = bfs.mapVertices((id, attr) => if(id == newRoot) 0.0 else attr)
			bfs = newGraph.pregel(initialMessage, maxIterations, activeEdgeDirection)(vprog, sendMessage, reduceMessage)
			println()
	
			unvisitedVertexes = bfs.vertices.filter(vertex => vertex._2 == Double.PositiveInfinity)
			unvisitedVertexesCount = unvisitedVertexes.count()
			connectedCounter += 1
		}

		bfs.unpersist(blocking = false)

		// print results
		//val bfsTime = calcTime(System.nanoTime, bfsStart)
		//log.info("BFS execution time: "+ bfsTime + " milliseconds")
		println("Computation is over. There are " + connectedCounter + " connected components!")
	

	}
}
