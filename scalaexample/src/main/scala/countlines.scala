import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.log4j.Logger


class graph(vertexArray: List[(Long, (Double))], edgesArray: List[org.apache.spark.graphx.Edge[Int]]){
  val vertices = vertexArray
  val edges = edgesArray

  def print_vertices(){
     println("The vertices are:")
     vertices.foreach(println)
  }

  def calcTime(s1:Long, s2:Long): String =  { BigDecimal((s1-s2)/1e6).setScale(2, BigDecimal.RoundingMode.HALF_UP).toString }
  val vprog = { (id: VertexId, attr: Double, msg: Double) => math.min(attr,msg) }
  val reduceMessage = { (a: Double, b: Double) => math.min(a,b) }

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

  def build_graph(sc: org.apache.spark.SparkContext){
    val vertexRDD = sc.parallelize(vertices)
    val edgeRDD   = sc.parallelize(edges)
    val log = Logger.getLogger(getClass.getName)
    val logPrefix = "DISTRIBUTED FOOLS:"
    val socialGraph = Graph(vertexRDD, edgeRDD)
    socialGraph.cache()
    val numPartitions: Int = socialGraph.edges.partitions.size
    val partitionStrategy = PartitionStrategy.fromString("EdgePartition2D")
    val rootVertex: VertexId = socialGraph.vertices.first()._1

    val initialGraph = socialGraph.partitionBy(partitionStrategy, numPartitions).mapVertices((id, attr) => if (id == rootVertex) 0.0 else Double.PositiveInfinity)
    socialGraph.unpersist(blocking = false)
		initialGraph.cache()
		val initialMessage = Double.PositiveInfinity
		val maxIterations = Int.MaxValue
		val activeEdgeDirection = EdgeDirection.Either

    val bfs = initialGraph.pregel(initialMessage, maxIterations, activeEdgeDirection)(vprog, sendMessage, reduceMessage)
    initialGraph.unpersist(blocking=false)
    val bfsStart = System.nanoTime()
    val bfsTime = calcTime(System.nanoTime, bfsStart)
		log.info(logPrefix +"BFS: Time: "+ bfsTime)

  }

}
object countlines {
  def main(args:Array[String]){
    val conf = new SparkConf().setAppName("Counting The Lines")
    val sc = new SparkContext(conf)
    val vertex = List((1L, (26.0)), (2L, ( 42.0)), (3L, (18.0)),
                     (4L, (16.0)), (5L, (45.0)), (6L, (30.0)),
                     (7L, (32.0)), (8L, ( 36.0)), (9L, (28.0)),
                     (10L, (48.0))
                  )
    val edges = List(Edge(1L, 2L, 1), Edge(2L, 3L, 1), Edge(3L, 1L, 1), Edge(3L, 4L, 1),                     Edge(3L, 5L, 1), Edge(4L, 5L, 1), Edge(6L, 5L, 1), Edge(7L, 6L, 1),                     Edge(6L, 8L, 1), Edge(7L, 8L, 1), Edge(7L, 9L, 1), Edge(9L, 8L, 1),                     Edge(8L, 10L, 1), Edge(10L, 9L, 1)                    )
    val obj = new graph(vertex, edges)
    obj.build_graph(sc)

  }
}
