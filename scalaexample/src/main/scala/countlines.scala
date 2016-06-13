import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.graphx._
import scala.collection.mutable.ListBuffer


class graph(vertexArray: List[(Long, (Double))], edgesArray: List[org.apache.spark.graphx.Edge[Int]]){
  val vertices = vertexArray
  val edges = edgesArray

  def build_graph(sc: org.apache.spark.SparkContext){
    val vertexRDD = sc.parallelize(vertices)
    val edgeRDD   = sc.parallelize(edges)
    val socialGraph = Graph(vertexRDD, edgeRDD)
    socialGraph.cache()

    // collecting the nieghbors of each vertex
    val neighborCollection =
      socialGraph.collectNeighborIds(EdgeDirection.Either).mapValues { (vid, nbrs) =>
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
      /* Uncomment the below line to see the neigbors of verticies */
      //neighborCollection.foreach(println(_))

      var triangle_count = 0

        /* Algorithm for Triangulr Counting */

        /* Loop Each Vertex , Select neighbor(one at a time) of a Vertex and Get the Neighbors of a selected Neighbor. Then Check
        whether the Neighbors of a selected vertex are present in the Neighbor List of a Selected Neighbor. If yes, increment the counter.
        At the nend , divide the count by 3 because If there exists a traingle between three Vertices then triangle will be calculated thrice
        while looping through each vertex. */

        neighborCollection.collect().zipWithIndex foreach { case(el, i) =>
             el._2.zipWithIndex foreach{case(value, index)=>
               var result = neighborCollection.filter{m => m._1 == value}
               result.collect().zipWithIndex foreach{ case(e,i) =>
                for (elem<- el._2.toArray){
                  if (e._2 contains elem){
                    triangle_count+= 1
                  }
                }
               }
             }
         }

       println("The number of triangles are "+ triangle_count/3)
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
