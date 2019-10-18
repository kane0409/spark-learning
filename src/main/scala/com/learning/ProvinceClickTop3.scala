package com.learning

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 需求：统计出每一个省份广告被点击次数的TOP3
  * 文件：agent.txt
  * 结构：时间戳，省份，城市，用户，广告，中间字段使用空格分割
  */
object ProvinceClickTop3 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("ProvinceClickTop3").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val line: RDD[String] = sc.textFile("input/agent.txt")
    //    line.foreach(println) //测试读取成功

    val result: RDD[(String, List[(String, Int)])] = line.map(_.split(" "))
      .map(f => ((f(1), f(4)), 1))
      .reduceByKey(_ + _)
      .map(f => (f._1._1, (f._1._2, f._2)))
      .groupByKey()
      .mapValues(_.toList.sortWith((x, y) => x._2 > y._2).take(3))

    result.foreach(println)

    sc.stop()
  }

}
