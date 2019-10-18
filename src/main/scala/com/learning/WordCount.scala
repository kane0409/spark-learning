package com.learning

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.textFile("./input/words.txt").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _, 1).sortBy(_._2, false)
    rdd.foreach(println(_))

    //测试创建rdd时默认分区数：4
    val rdd2: RDD[Int] = sc.parallelize(1 to 10)
    println(rdd2.partitions.size)

    sc.stop()
  }
}
