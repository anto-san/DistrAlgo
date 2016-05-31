import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.graphx._

object CountLines {
  def main(args:Array[String]){
    val conf = new SparkConf().setAppName("Counting The Lines")
    val sc = new SparkContext(conf)

    val vertexArray = Array((1L,("Alice","student")),(2L,("Bob","postdoc")),(3L,("Luisia","prof")),(5L,("Erica","prof")))
    val edgeArray = Array(Edge(1L,2L,"collab"),Edge(3L,1L,"advisor"),Edge(3L,5L,"friends"),Edge(2L,5L,"father"))

    val vertexRDD:RDD[(VertexId,(String,String))]= sc.parallelize(vertexArray)
    val edgeRDD:RDD[Edge[String]]= sc.parallelize(edgeArray)
    //Constructing the Graph
    val graph:Graph[(String,String),String] = Graph(vertexRDD,edgeRDD)

    var vertices  = graph.vertices.filter{case(id,(name,ocup)) => ocup=="prof"}.collect()
    var edges = graph.edges.filter{case Edge(srcID,destID,role) => role=="friends"}.collect()
    for((id,(name,ocup))<- graph.vertices.collect() ){
      println(name+" is "+ocup)
    }

    // Triplets are used to show the relation between vertices which is defined over edges
    val relation:RDD[String] = graph.triplets.map(triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
    relation.collect.foreach(println(_))
    // println("The number of vertices in a graph are"+ graph.vertices.count)
    
  }
}
