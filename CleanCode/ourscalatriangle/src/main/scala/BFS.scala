import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import scala.collection.mutable.ListBuffer

object BFS {

	/* utility to compute the difference between two timestamps in milliseconds */
	def calcTime(s1:Long, s2:Long): String = {
		BigDecimal((s1-s2)/1e6).setScale(2, BigDecimal.RoundingMode.HALF_UP).toString
	}
	/* Triangular Countig */

	def triangular_count(graph:Graph[Int,Int]){
		var t_count = 0
		// collecting the nieghbors of each vertex
		val neighborCollection =
			graph.collectNeighborIds(EdgeDirection.Either).mapValues { (vid, nbrs) =>
				val list = new ListBuffer[Long]()
				var i = 0
				while (i < nbrs.size) {
					if (nbrs(i) != vid) {
						list += nbrs(i)
					}
					i += 1
				}
				list.toList
			}

			/* Loop Each Vertex , Select neighbor(one at a time) of a Vertex and Get the Neighbors of a selected Neighbor. Then Check
			whether the Neighbors of a selected vertex are present in the Neighbor List of a Selected Neighbor. If yes, increment the counter.
			At the end , divide the count by 3 because If there exists a traingle between three Vertices then triangle will be calculated thrice
			while looping through each vertex. */
			neighborCollection.collect().zipWithIndex foreach { case(el, i) =>
				el._2.zipWithIndex foreach{case(value, index)=>
					var result = neighborCollection.filter{m => m._1 == value}
					result.collect().zipWithIndex foreach{ case(e,i) =>
						for (elem<- el._2.toArray){
							if (e._2 contains elem){
								t_count+= 1
							}
						}
					}
				}
			}
			println("The number of triangles are "+ t_count/6)
		}
		/* PREGEL - function applied to multiple incoming messages arriving to the same vertex, used before applying vprog */
		val reduceMessage = {
			(a: Double, b: Double) => math.min(a,b)
		}

		/* PREGEL - function used to compute a new vertex value, applied after the merging step of reduceMessage. */
		/* 	    attr is the vertex property and msg is the message */
		val vprog = {
			(id: VertexId, attr: Double, msg: Double) => math.min(attr,msg)
		}

		/* PREGEL - function used to create messages to send (and receivers) at supersteps end, for edges in the selected direction. */
		/* 	    In this case, if just one of the vertices is visited, send a message to the other vertex; otherwise do nothing.  */
		/* 	    It returns the iterator of a tuple (ReceiverVertexID, message) to send a message */
		val sendMessage = { (triplet: EdgeTriplet[Double, Int]) =>
			var iter:Iterator[(VertexId, Double)] = Iterator.empty
			val isSrcMarked = triplet.srcAttr != Double.PositiveInfinity
			val isDstMarked = triplet.dstAttr != Double.PositiveInfinity

			if(!(isSrcMarked && isDstMarked)){
				if(isSrcMarked){
					iter = Iterator((triplet.dstId,triplet.srcAttr+1))
				}else{
					iter = Iterator((triplet.srcId,triplet.dstAttr+1))
				}
			}
			iter
		}

	def main(args: Array[String]) {

		// set Spark configuration and context
		val conf = new SparkConf()
		conf.setAppName("SimpleName")
		/*This function you use only for test on your pc*/
		//conf.setMaster("local[*]")
		
		val sc = new SparkContext(conf)

		// record starting time and define logger
		val mainTime = System.nanoTime()
		val log = Logger.getLogger(getClass.getName)
		var t_count = 0

		val graph = GraphLoader.edgeListFile(sc,args(1))
		graph.cache()
		log.info("Graph loading time: " + calcTime(System.nanoTime, mainTime) + " milliseconds.")

		// define the Root vertex from which start BFS and initialize vertices' markings as visited or unvisited
		val rootVertex = graph.vertices.first()._1
		val initialGraph = graph.mapVertices((id, attr) => if (id == rootVertex) 0.0 else Double.PositiveInfinity)
		val bfsStart = System.nanoTime()
		// Unpersisting the previous graph and caching newly generated graph
		graph.unpersist(blocking = false)
		initialGraph.cache()

		// Pregel stuff
		// EdgeDirection.Either means that messages will be sent on the edge if any vertex of the triplet received some message
		
		triangular_count(graph:Graph[Int,Int])

		val bfsTime = calcTime(System.nanoTime, bfsStart)
		println("Triangle execution time: "+ bfsTime + " milliseconds")

	}
}
