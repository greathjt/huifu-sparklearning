package com.huifu.spark.ch5

import org.apache.spark.{SparkConf, SparkContext, SparkFiles}

/**
  * Created by chao.hu on 2016/12/15.
  */
object RddDemo {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("RddDemo").setMaster("local"))

    println(sc.appName)
    println(sc.getConf)
    println(sc.master)
    println(sc.version)

    val textFile = sc.textFile("hdfs://hadoop2:8020/tmp/conf.txt")
    val numbersRDD = textFile.flatMap(line => line.split(","))
    numbersRDD.collect().foreach(println)
  }
}
