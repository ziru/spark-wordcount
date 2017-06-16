package learn.spark

import org.apache.spark.{SparkConf, SparkContext}

object WordCountDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Word Count Demo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val inputPath = "file:///tmp/spark/4300-0.txt"
    val lines = sc.textFile(inputPath)
    val counts = lines.flatMap(line => line.split(" ")).map(x => (x, 1)).reduceByKey(_ + _)
    counts.take(10).foreach { case (w, c) => println(w, c) }
  }
}
