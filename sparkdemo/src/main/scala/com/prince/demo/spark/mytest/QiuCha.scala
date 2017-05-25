package com.prince.demo.spark.mytest

import org.apache.spark.sql.SparkSession

/**
  * 计算相邻元素差
  * Created by princeping on 2017/4/28.
  */
object QiuCha {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("QiuCha").getOrCreate()
    val input = spark.sparkContext.textFile("E:\\new.txt")
    val list = input.map(line => line.split(","))
    val num = list.map(line => {
      val line2 = line.tail
      val result = line.zip(line2).map(p => (p._2.toInt - p._1.toInt))
      result
    }).map(line => line.mkString("\n"))
    num.saveAsTextFile("E:\\999")
  }
}
