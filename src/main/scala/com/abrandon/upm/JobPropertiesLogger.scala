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
      val exMemStr = pairs.getOrElse("spark.executor.memory","1g")
      val exMem = exMemStr.take(exMemStr.length -1 ).toInt
      var drMemStr = pairs.getOrElse("spark.driver.memory","1g")
      val drMem = drMemStr.take(drMemStr.length -1 ).toInt
      var defaultYarnExecOverhead = exMem * 0.10
      if (defaultYarnExecOverhead < 384) defaultYarnExecOverhead = 384
      var defaultYarnDrivOverhead = exMem * 0.10
      if (defaultYarnDrivOverhead < 384) defaultYarnExecOverhead = 384

      fw.write(message + ";" + sc.applicationId + ";" + final_time() + ";" + pairs.getOrElse("spark.driver.cores","1") +
        ";" + pairs.getOrElse("spark.executor.cores","1") + ";" +  pairs.getOrElse("spark.yarn.executor.memoryOverhead",defaultYarnExecOverhead.toString) +
        ";" + pairs.getOrElse("spark.yarn.driver.memoryOverhead",defaultYarnDrivOverhead.toString) + ";" + pairs.getOrElse("spark.executor.memory","1g") +
        ";" + pairs.getOrElse("spark.driver.memory","1g") + ";" + pairs.getOrElse("spark.reducer.maxSizeInFlight","48m") +
        ";" + pairs.getOrElse("spark.shuffle.compress","true")  + ";" + pairs.getOrElse("spark.shuffle.file.buffer","32k") +
        ";" + pairs.getOrElse("spark.shuffle.manager","sort") + ";" + pairs.getOrElse("spark.shuffle.spill.compress","true") +
        ";" + pairs.getOrElse("spark.io.compression.codec","snappy") + ";" +  pairs.getOrElse("spark.memory.fraction","0,75") +
        ";" + pairs.getOrElse("spark.memory.storageFraction","0,5") + ";" + pairs.getOrElse("spark.default.parallelism",sc.defaultParallelism.toString) +
        ";" + pairs.getOrElse("spark.executor.heartbeatInterval","10s") + ";" + pairs.getOrElse("spark.task.cpus","1") + "\n")
    }

    finally fw.close()

  }

}
