package com.huifu.spark.ch5

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by chao.hu on 2016/12/15.
  */
object ReadSequence {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("ReadSequence").setMaster("local"))
    println(sc.appName)
    println(sc.getConf)
    println(sc.master)
    println(sc.version)
//    val sequenceFile = sc.sequenceFile[Class[ImmutableBytesWritable], Class[Result]]("hdfs://hadoop2:8020/tmp/output/part-m-00000")
//    println(sequenceFile.take(1))

  }
}
