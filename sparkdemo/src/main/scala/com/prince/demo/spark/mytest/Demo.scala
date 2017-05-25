package com.prince.demo.spark.mytest

import org.apache.spark.sql.SparkSession

/**
  * Created by princeping on 2017/5/23.
  */
object Demo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("DemoThree").getOrCreate()
    val sc = spark.sparkContext

    val input = sc.textFile("C:\\Users\\Administrator\\Desktop\\新建文件夹\\0501.log")

    val restRDD = input
      .map(line => (line.split("\\|")(0).split(",")(0),line.split("\\|")(3),line.split("\\|")(6),line.split("\\|")(10)))
      .filter(line => line._2.contains("rest-01.bxd365.com"))

    val rdd1 = restRDD
      .filter(line => line._3.contains("/getWelcomeImages"))
      .map(line => line._1+"\t"+line._3+"\t"+line._4)

    val rdd3 = restRDD
      .filter(line => line._3.contains("/V6_getIndexad") | line._3.contains("/api/showPlanbook/check")
        | line._3.contains("/activity_log") | line._3.contains("/api/listenDaily/itemlist")
        | line._3.contains("/api/indexs/navigation") | line._3.contains("/bxdInsurProduct/lists"))
      .map(line => line._1+"\t"+line._3+"\t"+line._4)

    val rdd4 = restRDD
      .filter(line => line._3.contains("/api/elite/lists") | line._3.contains("/api/weiyuedu/lists")
        | line._3.contains("/api/clause") | line._3.contains("/api/weiyuedu"))
      .map(line => line._1+"\t"+line._3+"\t"+line._4)

    //rdd1.repartition(1).saveAsTextFile("E:\\new\\欢迎页")
    rdd3.repartition(1).saveAsTextFile("E:\\new\\欢迎页")
    //rdd4.repartition(1).saveAsTextFile("E:\\new\\学习")
  }
}
