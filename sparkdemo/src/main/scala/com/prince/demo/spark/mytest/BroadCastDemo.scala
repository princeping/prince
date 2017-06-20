package com.prince.demo.spark.mytest

import org.apache.spark.sql.SparkSession

/**
  * 利用广播变量进行多关键词过滤
  * Created by princeping on 2017/5/19.
  */
object BroadCastDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("BroadCastDemo").getOrCreate()
    val sc = spark.sparkContext

    val input = sc.textFile("C:\\Users\\Administrator\\Desktop\\test.txt")
    val input1 = sc.textFile("C:\\Users\\Administrator\\Desktop\\tes.txt")

    val broads = sc.broadcast(input1.collect())

    val out = input.map(x => {
      var str:String = ""
      val filter = broads.value.foreach(y =>{
        if(x.contains(y))
          str = x
      })
      str
    }).filter(_.length>0)

    out.repartition(1).saveAsTextFile("C:\\Users\\Administrator\\Desktop\\out")

    spark.stop()
  }
}
