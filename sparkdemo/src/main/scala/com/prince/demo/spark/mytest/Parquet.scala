package com.prince.demo.spark.mytest

import org.apache.spark.sql.SparkSession

/**
  * Parquet列存储转换
  * Created by princeping on 2017/5/22.
  */
object Parquet {

  case class NginxLog(ip:String, server_address:String, time_local:String, url:String, status:Int, user_agent:String)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("ParquetDemo").getOrCreate()

//    val sc = spark.sparkContext
//    val input = sc.textFile("C:\\Users\\Administrator\\Desktop\\新建文件夹\\0426.log")
//    val parquetRDD = input
//      .map(line => line.split("\\|")(0).split(",")(0)+"\t"+line.split("\\|")(3)+"\t" +line.split("\\|")(5)+"\t"
//        +line.split("\\|")(6)+"\t"+line.split("\\|")(7)+"\t"+line.split("\\|")(10))
//
//    import spark.implicits._
//
//    val parquetDF = parquetRDD.map(line => {
//      val array = line.split("\t")
//      NginxLog(array(0), array(1), array(2), array(3), array(4).toInt, array(5))
//    }).toDF
//
//    parquetDF.repartition(1).write.parquet("E:\\LogA\\nginx\\parquet\\20170426")

    val input = spark.read.parquet("E:\\LogA\\nginx\\parquet\\20170429\\part-*")
    input.printSchema()
    spark.stop()
  }
}
