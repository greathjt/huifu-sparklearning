package com.huifu.spark.ch5

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by chao.hu on 2016/12/15.
  */
object ReadHbase {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("ReadHbase").setMaster("local"))
    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, "BD_PAGE_REPOSITORY")

    val hBaseRDD = sc.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    System.exit(0)
  }
}
