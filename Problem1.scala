package comp9313.ass3

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Problem1 {
  def main(args: Array[String]) {
    val inputFile = args(0)
    val outputFolder = args(1)
    val conf = new SparkConf().setAppName("problem1").setMaster("local")
    val sc = new SparkContext(conf)
    val input = sc.textFile(inputFile)
//    get the graph information
    val nodesMap = input.map(_.split(" ")).map(x => (x(1).toLong, (x(3).toDouble, 1)))
//    get the total length to out-going edges and the quantity of next nodes for each node
    val nodesReduce = nodesMap.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
//    calculate the average length of each node
    val nodesCal = nodesReduce.map(x => (x._1, x._2._1/x._2._2)).sortBy(x => (x._2, -x._1), false)
//    covert the results to desired format
    val nodesOut = nodesCal.map(x => x._1 + "\t" + x._2)
    nodesOut.saveAsTextFile(outputFolder)
  }
}
