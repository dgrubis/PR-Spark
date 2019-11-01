package hw4.pr_spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object PageRankSparkMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nRS_RMain <input dir> <output dir>")
      System.exit(1)
    }
    
    

    
    
}
}
