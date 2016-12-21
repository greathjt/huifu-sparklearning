package com.huifu.spark.ch5

import java.io.StringReader

import au.com.bytecode.opencsv.CSVReader
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by chao.hu on 2016/12/15.
  */
object RddDemo2 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("RddDemo").setMaster("local"))
    println(sc.appName)
    println(sc.getConf)
    println(sc.master)
    println(sc.version)
    val textFile = sc.textFile("hdfs://hadoop2:8020/tmp/a.txt")
    val splitLines = textFile.map(
      line => {
        val reader = new CSVReader(new StringReader(line))
        reader.readNext()
      }
    )
    val numericData = splitLines.map(line => line.map(_.toDouble))
    val summedData = numericData.map(row => row.sum)
    println(summedData.collect().mkString(","))
  }
}

