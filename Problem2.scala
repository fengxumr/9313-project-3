package comp9313.ass3

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Problem2 {
  def main(args: Array[String])= {
    val inputFile = args(0)
    val srcNode = args(1).toLong
    val conf = new SparkConf().setAppName("problem2").setMaster("local")
    val sc = new SparkContext(conf)
    val data = sc.textFile(inputFile).map(_.split(" "))
//    Create an RDD for the vertices, set the distance from node "t" to corresponding nodes as infinity double
    val vertexRDD: RDD[(Long, Double)] = data.map(x => (x(1).toLong, Double.PositiveInfinity)) ++ data.map(x => (x(2).toLong, Double.PositiveInfinity))
//    Create an RDD for edges
    val edgeRDD: RDD[Edge[Double]] = data.map(x => Edge(x(1).toLong, x(2).toLong, x(3).toDouble))
//    Build the Graph
    val graph = Graph(vertexRDD, edgeRDD)
//    Build the Graph that initial the distance from "t" to "t" as 0.0
    val initialGraph = graph.mapVertices((id, _) => if (id == srcNode) 0.0 else Double.PositiveInfinity)
//    Calculate the shortest distance from "t" to all nodes (including "t")
    val sssp = initialGraph.pregel(Double.PositiveInfinity) (
      (id, dist, newDist) => math.min(dist, newDist),
      triplet => {
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b)
    )
//    output the number of nodes that can be reached from "t", excluding itself
    val result = sssp.vertices.filter{case(id, num) => num != Double.PositiveInfinity}.count()
    if (result == 0) {
      println(0)
    } else {
      println(result - 1)
    }
  }
}
