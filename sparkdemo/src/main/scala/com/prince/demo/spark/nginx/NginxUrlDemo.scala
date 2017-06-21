package com.prince.demo.spark.mytest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by princeping on 2017/6/15.
  */
object UrlDemo {

  Logger.getLogger("org").setLevel(Level.WARN)
  case class Host(host:String, status:Int, time:Double)
  case class Url(url:String, status:Int, time:Double)
  def main(args: Array[String]): Unit = {

    //val spark = SparkSession.builder().master("spark://master1.hadoop:7077").appName("UrlDemo").getOrCreate()
    val spark = SparkSession.builder().master("local[2]").appName("UrlDemo").getOrCreate()

    val sc = spark.sparkContext

    //val input = sc.textFile("hdfs://master1.hadoop:9000/prince/data/20170616/access_20170616_*.log")
    val input = sc.textFile("E:\\LogA\\nginx\\data\\log\\nginx\\access_20170616_*.log")

    val primaryRDD = input
      .filter(_.split("\\|").length == 14)
      .filter(_.split("\\|")(7).contains(" /"))
      .filter(_.split("\\|")(7).contains(" HTTP"))
      .filter(_.split("\\|")(8).length==3)
      .filter(_.split("\\|")(12).length<6)
      .filter(_.split("\\|")(12).contains("."))
      .map(line => (line.split("\\|")(4).replaceAll("bxd[0-9]\\d*.bxd365.com","*.bxd365.com")+"\t"+
        line.split("\\|")(8).replaceAll("[^0-9]","")+"\t"+line.split("\\|")(12).replaceAll("[^0-9.]",""),
        line.split("\\|")(7).substring(line.split("\\|")(7).indexOf(" ")+1, line.split("\\|")(7).lastIndexOf(" "))))
      .filter(_._2.contains("/site/getsmscode").equals(false))
      .filter(_._2.length>2)

//    val primaryRDD = input
//      .map(_.replace("\"", ""))
//      .map(_.replace(", ", ","))
//      .filter(_.split("\\|")(6).contains(" /"))
//      .filter(_.split("\\|")(6).contains(" HTTP"))
//      .filter(_.split("\\|")(7).length==3)
//      .filter(_.split("\\|")(11).length<6)
//      .filter(_.split("\\|")(11).contains("."))
//      .map(line => (line.split("\\|")(3)+"\t"+ line.split("\\|")(7)+"\t"+line.split("\\|")(11),
//        line.split("\\|")(6).substring(line.split("\\|")(6).indexOf(" ")+1, line.split("\\|")(6).lastIndexOf(" "))))
//      .filter(_._2.contains("/site/getsmscode").equals(false))
//      .filter(_._2.length>2)

    val rdd1 = primaryRDD.filter(_._2.contains("?")).map(line => (line._1, line._2.substring(0, line._2.indexOf("?"))))
    val rdd2 = primaryRDD.filter(line => line._2.contains("?").equals(false))
    val result = rdd1.union(rdd2)

    val filterRDD = result.map(line =>{
      if(line._2.contains(".jpg") | line._2.contains(".png") | line._2.contains(".gif")
        | line._2.contains(".css") | line._2.contains(".html") | line._2.contains(".js")
        | line._2.contains("index.php")){
        val str = line._2.substring(line._2.indexOf("/"), line._2.lastIndexOf("/"))
        (line._1, str)
      }
      else
        line
    })

    import spark.implicits._

    val urlDF = filterRDD.map(line => {
      val temp = line._1.split("\t")
      Url(temp(0)+line._2, temp(1).toInt, temp(2).toDouble)
    }).toDF().cache()

    val hostDF = primaryRDD.map(line => {
      val temp = line._1.split("\t")
      Host(temp(0), temp(1).toInt, temp(2).toDouble)
    }).toDF.cache()

    urlDF.createOrReplaceTempView("url_table1")
    hostDF.createOrReplaceTempView("host_table1")

    val out1 = spark.sql("select url,count(1) as a," +
      "cast(sum(case when status>=200 and status<400 then 1 else 0 end) as int) as rate0," +
      "cast(sum(case when status>=400 and status<600 and status!=404 then 1 else 0 end) as int) as rate1," +
      "cast(sum(case when status!=200 and status!=301 and status!=302 and status!=303 and status!=304 and " +
      "status!=401 and status!=403 and status!=500 and status!=503 and status!=504 and status!=591 and " +
      "status!=499 then 1 else 0 end) as int) as rate2," +
      "cast(sum(case when time<1 then 1 else 0 end) as int) as cnt0," +
      "cast(sum(case when time>=1 then 1 else 0 end) as int) as cnt1," +
      "cast(sum(case when time>=2 then 1 else 0 end) as int) as cnt2," +
      "cast(sum(case when time>=3 then 1 else 0 end) as int) as cnt3 " +
      "from url_table1 group by url")

    val out2 = spark.sql("select host," +
      "cast(sum(case when status>=200 and status<400 then 1 else 0 end) as int) as rate0," +
      "cast(sum(case when status>=400 and status<600 and status!=404 then 1 else 0 end) as int) as rate1," +
      "cast(sum(case when status!=200 and status!=301 and status!=302 and status!=303 and status!=304 and" +
      " status!=401 and status!=403 and status!=500 and status!=503 and status!=504 and status!=591 and " +
      "status!=499 then 1 else 0 end) as int) as rate2," +
      "cast(sum(case when time<1 then 1 else 0 end) as int) as cnt0," +
      "cast(sum(case when time>=1 then 1 else 0 end) as int) as cnt1," +
      "cast(sum(case when time>=2 then 1 else 0 end) as int) as cnt2," +
      "cast(sum(case when time>=3 then 1 else 0 end) as int) as cnt3 " +
      "from host_table1 group by host")

    out1.createOrReplaceTempView("url_table2")
    out2.createOrReplaceTempView("host_table2")

    val output1 = spark.sql("select url,(cnt0+cnt1) as a,rate0/(rate0+rate1+rate2),rate1/(rate0+rate1+rate2)," +
      "rate2/(rate0+rate1+rate2),cnt1/(cnt0+cnt1),cnt2/(cnt0+cnt1),cnt3/(cnt0+cnt1) from url_table2 order by a DESC")

    val output2 = spark.sql("select host,(cnt0+cnt1) as a,rate0/(rate0+rate1+rate2),rate1/(rate0+rate1+rate2)," +
      "rate2/(rate0+rate1+rate2),cnt1/(cnt0+cnt1),cnt2/(cnt0+cnt1),cnt3/(cnt0+cnt1) from host_table2 order by a DESC")

    output1.repartition(1).write.csv("E:\\NB\\0616\\url")
    output2.repartition(1).write.csv("E:\\NB\\0616\\host")
//    output1.repartition(1).write.csv("hdfs://master1.hadoop:9000/prince/output/0616/url")
//    output2.repartition(1).write.csv("hdfs://master1.hadoop:9000/prince/output/0616/host")
  }
}
