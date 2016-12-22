package com.huifu.spark.ch5

import java.io.StringReader

import au.com.bytecode.opencsv.CSVReader
import org.apache.spark.SparkContext

/**
  * Created by chao.hu on 2016/12/22.
  */
case class DataPoint(x: Double, y: Double)

object GeoIpExample {
  def main(args: Array[String]): Unit = {
    val master = args[
    0
    ]
    val inputFile = args[
    1
    ]
    val iterations = 100
    val maxMindPath = "GeoLiteCity.dat"

    val sc = new SparkContext(master, "GeoIpExample", System.getenv("SPRK_HOME"), Seq(System.getenv("JARS")))
    val invalidLineCounter = sc.accumulable(0)
    val inFile = sc.textFile(inputFile)
    val parsedInput = inFile.flatMap(line => {
      try {
        val row = (new CSVReader(new StringReader(line))).readNext()
        Some((row(0), row.drop(1).map(_.toDouble)))
      } catch {
        case _: Throwable => {
          invalidLineCounter += 1
          None
        }
      }
    })
    val geoFile = sc.addFile(maxMindPath)
    val ipCountries = parsedInput.flatMapWith( _=>IpGeo())

  }
}
