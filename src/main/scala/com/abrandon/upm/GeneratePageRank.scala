package com.abrandon.upm

/**
 * Created by alvarobrandon on 21/12/15.
 */


import org.apache.spark.{SparkContext, SparkConf}

import scala.util.Random
import java.io._

object GeneratePageRank {

  def main (args: Array[String]) {
    // Arguments: NumberOfVertex, NumberOfEdges

    val sparkConf = new SparkConf().setAppName("GeneratePageRank")
    val sc = new SparkContext(sparkConf)

    if (args.length != 3) {
      System.err.println("Usage: GeneratePageRank <NumberOfVertex> <NumberOfEdges> <save_path>")
      System.exit(1)
    }
    val nNodes = args(0).toInt
    val nEdges = args(1).toInt
    val save_path = args(2)

    def generateNodeNames(nNodes: Int): Vector[String] = {
      var nodes: Set[String] = Set()
      val r = new Random()
      while (nodes.size < nNodes) {
        nodes += r.nextInt(1000000).toString()
      }
      nodes.toVector
    }

    val nodes = generateNodeNames(nNodes)

    val rnd = new Random()

    var e: Set[(String)] = Set()
    while (e.size < nEdges) {
      val v1 = nodes(rnd.nextInt(nodes.size))
      val v2 = nodes(rnd.nextInt(nodes.size))
      if (!v1.equals(v2)) {
        e += v1 + " " + v2
      }
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
    val data = sc.parallelize(e.toSeq)
    data.coalesce(1,shuffle = true).saveAsTextFile(save_path)
    sc.stop()
  }


}
