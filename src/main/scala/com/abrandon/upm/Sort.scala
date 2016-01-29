package com.abrandon.upm

/**
 * Created by alvarobrandon on 21/12/15.
 */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object Sort {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: Sort <data_file> <save_file>" +
        " [<slices>]")
      System.exit(1)
    }

    var splits = 2
    val conf = new SparkConf().setAppName("BigDataBench Sort")
    val spark = new SparkContext(conf)
    val logger = new JobPropertiesLogger(spark,"/home/abrandon/log.csv")
    val filename = args(0)
    val save_file = args(1)
    if (args.length > 2) splits = args(2).toInt
    val lines = spark.textFile(filename, splits)
    logger.start_timer()
    val data_map = lines.map(line => {
      (line, 1)
    })

    val result = data_map.sortByKey().map { line => line._1}
    logger.stop_timer()
    logger.write_log("Sort By Key: Sort App")
    result.saveAsTextFile(save_file)

    println("Result has been saved to: " + save_file)
  }



}
