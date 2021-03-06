package com.abrandon.upm

/**
 * Created by alvarobrandon on 17/12/15.
 */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils


object SupportVectorMachine {


  def main (args: Array[String]){


    if (args.length != 2) {
      // scalastyle:off println
      println("Usage: SupportVectorMachine " +
        "<input_file> <num_iterations> <slices>")
      // scalastyle:on println
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("SupporVectorMachine")
    val sc = new SparkContext(conf)
    val logger = new JobPropertiesLogger(sc,"/home/abrandon/log.csv")

    var slices = 1
    val inputFile = args(0)
    val nIterations = args(1).toInt
    if (args.length > 2){ slices = args(2).toInt}

    val data = sc.textFile(inputFile,slices)
    logger.start_timer()
    val lPointsData = data.map(l=>LabeledPoint.parse(l))

    val splits = lPointsData.randomSplit(Array(0.6,0.4),seed= 11L)
    val training = splits(0).cache()
    val test = splits(1)



    val model = SVMWithSGD.train(training,nIterations)
    logger.stop_timer()
    logger.write_log("Train Model: SVM App")
    model.clearThreshold()

    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()
    println("Area under ROC = " + auROC)





  }

}
