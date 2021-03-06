package com.abrandon.upm

/**
 * Created by alvarobrandon on 16/12/15.
 */


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.KMeansDataGenerator


object GenerateKMeansData {

  def main (args: Array[String]){
    if ((args.length != 6)){
      System.err.println(
        s"Usage: $GenerateKMeansData <Output_data_URL> <NUM_POINTS> " +
          s"<NUM_CLUSTERS> <NUM_DIMENSIONS> <RATIO_SPREAD_CENTERS> <NUM_PARTITIONS>"
      )
      System.exit(1)
    }
    val fileURL = args(0)
    val nPoints = args(1).toLong
    val nClusters = args(2).toInt
    val nDim = args(3).toInt
    val ratio = args(4).toDouble
    val nPart = args(5).toInt

    val nIters = nPoints / Int.MaxValue // We have to add this for Int too big Errors

    /*

    val fileURL = "kMeansData"
    val nPoints = 10000
    val nClusters = 4
    val nDim = 5
    val ratio = 5
    val nPart = 1

     */

    val conf = new SparkConf().setAppName("KMeansDataGenerator")
    val sc = new SparkContext(conf)
    if (nIters == 0) {
      val points = KMeansDataGenerator.generateKMeansRDD(sc, nPoints.toInt, nClusters,nDim,ratio,nPart) //generateKMeansRDD(SparkContext sc, int numPoints, int k, int d, double r, int numPartitions)
      val data = points.map(x => Vectors.dense(x)).cache()
      val dataString = data.map(l => l.toString)
      dataString.saveAsTextFile(fileURL)
    }
    else{
      val points = KMeansDataGenerator.generateKMeansRDD(sc, Int.MaxValue, nClusters,nDim,ratio,nPart) //generateKMeansRDD(SparkContext sc, int numPoints, int k, int d, double r, int numPartitions)
      var data = points.map(x => Vectors.dense(x))
      for (i <- 1 to nIters.toInt) {
        val points = KMeansDataGenerator.generateKMeansRDD(sc, Int.MaxValue, nClusters, nDim, ratio, nPart)
        data = data.union(points.map(x => Vectors.dense(x)))
      }
      val dataString = data.map(l => l.toString)
      dataString.saveAsTextFile(fileURL)
    }

    sc.stop()

    //
    //    res.coalesce(1,shuffle = true).saveAsTextFile(fileURL)
    //    sc.stop()

  }


}
