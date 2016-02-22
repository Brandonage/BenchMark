package com.abrandon.upm

/**
 * Created by alvarobrandon on 21/12/15.
 */


import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkContext, SparkConf}

import scala.util.Random
import java.io._

object GeneratePageRank {

  def main (args: Array[String]) {
    // Arguments: NumberOfVertex, NumberOfEdges

    val sparkConf = new SparkConf().setAppName("GeneratePageRank")
    val sc = new SparkContext(sparkConf)

    if (args.length != 4) {
      System.err.println("Usage: GeneratePageRank <NumberOfVertex> <NumberOfEdges> <save_path> <parts>")
      System.exit(1)
    }
    val nNodes = args(0).toLong
    val nEdges = args(1).toLong
    val save_path = args(2)
    val parts = args(3).toInt

    /*    def generateNodeNames(nNodes: Long): Vector[String] = {
          var nodes: Set[String] = Set()
          val r = new Random()
          while (nodes.size < nNodes) {
            nodes += r.nextLong().toString()
          }
          nodes.toVector
        }*/

    def generateEdge():String = {
      val rnd = new scala.util.Random()
      val v1 = rnd.nextInt(10000000)
      val v2 = rnd.nextInt(10000000)
      val res = v1.toString + " " + v2.toString
      return res
    }

    //    val nodes = generateNodeNames(nNodes)
    //
    //    val broadcastNodes = sc.broadcast(nodes)
    val nIters = nEdges / Int.MaxValue


    if (nIters == 0) {
      val data = sc.parallelize(1L to nEdges.toInt,parts).map(x => generateEdge()) //generateKMeansRDD(SparkContext sc, int numPoints, int k, int d, double r, int numPartitions)
      data.coalesce(1,shuffle = true).saveAsTextFile(save_path)
    }
    else{
      val excess = nEdges - (Int.MaxValue.toLong * nIters)
      var data = sc.parallelize(1L to excess.toInt,parts).map(x => generateEdge()) //generateKMeansRDD(SparkContext sc, int numPoints, int k, int d, double r, int numPartitions)
      for (i <- 2 to nIters.toInt) {
        val data2 = sc.parallelize(1L to Int.MaxValue - 1).map(x => generateEdge())
        data = data.union(data2)
      }
      data.coalesce(1,shuffle = true).saveAsTextFile(save_path)
    }


    /*
    val rnd = new Random()

    val data: RDD[(String)] = sc.parallelize(1 to nEdges,parts).map{idx =>
      val v1 = nodes(rnd.nextInt(nodes.size))
      val v2 = nodes(rnd.nextInt(nodes.size))
      if (!v1.equals(v2)) {
        e += v1 + " " + v2
      }
      e
    }
*/
    sc.stop()
  }
}
