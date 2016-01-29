package com.abrandon.upm

/**
 * Created by alvarobrandon on 16/12/15.
 */



import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors

object KMeansBench {
  val MLLibKMeans = org.apache.spark.mllib.clustering.KMeans

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      //System.err.println("Usage: KMeans <master> <data_file> <k> <iterations> <save_path>" +
      System.err.println("Usage: KMeans <data_file> <k> <iterations>" +
        " [<slices>]")
      System.exit(1)
    }

    var splits = 2
    val conf = new SparkConf().setAppName("KMeans")
    val sc = new SparkContext(conf)
    val logger = new JobPropertiesLogger(sc,"/home/abrandon/log.csv")


    val filename = args(0)
    val k = args(1).toInt
    val iterations = args(2).toInt // default 20
    // val save_file = args(4)
    //if (args.length > 5) splits = args(5).toInt
    if (args.length > 3) splits = args(3).toInt

    println("Start KMeans training...")
    // Load and parse the data
    val data = sc.textFile(filename, splits)
    //Data is an RDD of strings.
    // With drop we take each string and drop the [  and ] symbols of the beginning and the end of each string
    // We split then to have an Array of Strings that we map to Doubles
    val parsedData = data.map(s => s.drop(1).dropRight(1).split(",").map(_.toDouble))

    val trainData = parsedData.map(s => Vectors.dense(s))

    logger.start_timer()
    val clusters = MLLibKMeans.train(trainData, k, iterations)
    logger.stop_timer()
    logger.write_log("Training Model: KMeans App")
    //val clusters = org.apache.spark.mllib.clustering.KMeans.train(parsedData, k, iterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(trainData)
    println("OK. Within Set Sum of Squared Errors = " + WSSSE)
  }
}