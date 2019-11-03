package hw4.pr_spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import scala.math.pow

object PageRankSparkMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nPageRankSparkMain <input dir> <output dir>")
      System.exit(1)
    }
    
    val conf = new SparkConf()
    val sparkSession = SparkSession.builder
                       .appName("PageRankSpark")
                       .config(conf)
                       .getOrCreate()
    val max_iter = if (args.length > 1) args(1).toInt else 10
    val k: Int = 4
    
    val lines = sparkSession.read.textFile(args(0)).rdd
    val ids = lines.map{ s =>
      val parts = s.split(",")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache() //assume a file is loaded in for now
    //cache in order to tell Spark to not recompute the graph rdd at each iteration
    
    val idsDangling = ids.flatMap{case(node, adjList) => if (adjList == None) "0" else adjList}
    //handles dangling nodes by assigning to a dummy node if the adjacency list is empty (has no outgoing edges)
    var PR = ids.mapValues(v => if (v!= 0) (1.0 / pow(k,2)) else 0) //set the initial pageRank values as 1/k^2 except the dummy vertex 0 should start as 0
    
    
    for (i <- 1 to max_iter) { //loop through max iterations
      val contr = ids.join(PR).values.flatMap{case (nodes, pr) => //inner join the graph with the pagerank values
        val size = nodes.size
        nodes.map(node => (node, pr / size))
      }
      PR = contr.reduceByKey(_+_).mapValues(0.15 + 0.85 * _) //update the pagerank values
      PR.toDebugString //add todebugstring to log files after each iteration
    }

    val results = PR.collect()
    results.foreach(tup => println(s"${tup._1} has rank:  ${tup._2} ."))
    //print out the final pagerank value for each node

    sparkSession.stop()
    
}
}
