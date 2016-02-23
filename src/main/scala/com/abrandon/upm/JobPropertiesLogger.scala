package com.abrandon.upm


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io.FileWriter

/**
  * Created by abrandon on 17/02/16.
  */
class JobPropertiesLogger(sc:SparkContext,fileName:String) {
  var start :Long = 0
  var end: Long = 0

  def start_timer(): Unit ={
    start=System.currentTimeMillis()
  }

  def stop_timer(): Unit = {
    end=System.currentTimeMillis()
  }

  def final_time(): String ={
    return (end-start).toString
  }

  def write_log(message:String): Unit ={
    val pairs = sc.getConf.getAll.toMap
    val fw = new FileWriter(fileName,true)
    try{
      fw.write(message + ";" + final_time() + ";" + pairs.getOrElse("spark.memory.storageFraction","0.5") + ";" +
        pairs.getOrElse("spark.shuffle.spill.compress","true") + ";" + pairs.getOrElse("spark.io.compression.codec","snappy") +
        ";" + pairs.getOrElse("spark.executor.memory","1g") + ";\n")
    }

    finally fw.close()

  }

}
