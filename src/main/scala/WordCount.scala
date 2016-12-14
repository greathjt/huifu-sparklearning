import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by chao.hu on 2016/12/14.
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count").setMaster("local[3]"))
    val threshhold = args(1).toInt
    val tokenized = sc.textFile(args(0)).flatMap(_.split(" "))
    val wordCounts = tokenized.map(str => (str, 1)).reduceByKey(_ + _)
    val filtered = wordCounts.filter((_._2 >= threshhold))
    val charCount = filtered.flatMap(_._1.toCharArray).map((_, 1)).reduceByKey(_ + _)
    System.out.println(charCount.collect().mkString(", "))
  }
}
