package com.abrandon.upm

/**
 * Created by alvarobrandon on 17/12/15.
 */


import scala.util.Random

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

object GenerateSVMData {

  def main (args: Array[String]){
    // GenerateSVMData.main(Array("<output_dir>", "[num_examples]", "[num_features]", "[num_partitions]")
    // GenerateSVMData.main(Array("SVM", "10000","5","4"))
    if (args.length < 4) {
      // scalastyle:off println
      println("Usage: GenerateSVMData " +
        "<output_dir> <num_examples> <num_features> <num_partitions>")
      // scalastyle:on println
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("KMeansDataGenerator")
    val sc = new SparkContext(conf)

    val outputPath: String = args(0)
    val nexamples: Int = if (args.length > 1) args(1).toInt else 1000
    val nfeatures: Int = if (args.length > 2) args(2).toInt else 2
    val parts: Int = if (args.length > 3) args(3).toInt else 2

    val globalRnd = new Random(94720)
    val trueWeights = Array.fill[Double](nfeatures + 1)(globalRnd.nextGaussian())

    val data: RDD[String] = sc.parallelize(0 until nexamples, parts).map { idx =>
      val rnd = new Random(42 + idx)
      val x = Array.fill[Double](nfeatures) {
        rnd.nextDouble() * 2.0 - 1.0
      }

      val yD = rnd.nextGaussian() * 0.1
      val y = if (yD < 0) 0 else 1

      val lPoint = LabeledPoint(y, Vectors.dense(x))
      lPoint.toString()
    }

    data.coalesce(1,shuffle = true).saveAsTextFile(outputPath)


    //sc.stop()
  }

}
