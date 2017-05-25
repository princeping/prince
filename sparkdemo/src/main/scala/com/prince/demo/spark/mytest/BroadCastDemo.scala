package com.prince.demo.spark.mytest

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by princeping on 2017/5/19.
  */
object BroadCastDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("BroadCastDemo").getOrCreate()
    val sc = spark.sparkContext

//    val broads = sc.broadcast(3)
//    val lists = List(1,2,3,4,5)
//    val listRDD = sc.parallelize(lists)
//    val result = listRDD.filter(x=>x*broads.value>10)
//    result.foreach(x=>println("增加后的结果:" + x))

//    val array:Array[String] = Array("POST", "GET")
//    val broadcast = sc.broadcast(array)
    val array:Array[String] = Array("POST", "GET")
    val rdd:RDD[String] = sc.textFile("C:\\Users\\Administrator\\Desktop\\新建文件夹\\0430.log")
//    val filterRDD = rdd.filter(line => line.split("\\|")(6).contains(array))
//    filterRDD.repartition(1).saveAsTextFile("E:\\new")
    for(i<-0 until array.length){
      val filterRDD = rdd.filter(line => line.split("\\|")(6).contains(array(i)))
    }
    spark.stop()
  }
}
