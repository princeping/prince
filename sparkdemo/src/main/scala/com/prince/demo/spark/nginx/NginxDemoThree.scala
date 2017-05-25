package com.prince.demo.spark.nginx

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.sql.SparkSession
import scala.util.matching.Regex

/**
  * 格式化Nginx日志，求取pv、uv等
  * Created by princeping on 2017/5/2
  */
object NginxDemoThree {
  def main(args: Array[String]): Unit = {

    //val spark = SparkSession.builder().master("local").appName("NginxDemoThree").getOrCreate()
    val spark = SparkSession.builder().master("spark://master1.hadoop:7077").appName("NginxDemoThree").getOrCreate()

    //val input = spark.sparkContext.textFile("E:\\LogA\\nginx\\data\\log\\nginx\\20w.log")
    //val input = spark.sparkContext.textFile("E:\\LogA\\nginx\\data\\log\\nginx\\access_20170427_*.log")
    val input = spark.sparkContext.textFile("hdfs://master1.hadoop:9000/prince/data/20170427/access_20170427_*.log")

    /*格式化日志*/
    val primaryRDD = input.map(line => {
      val pattern = new Regex("\".*?\"")
      val str = (pattern findAllIn line).mkString("\t").replace("\"", "") + "\t" +
        line.substring(line.lastIndexOf("\"")+1, line.length).trim.split(" ").mkString("\t")
      val result = str.replace("-\t-\t[", "[")
      result
    })

    /*过滤出有效日志*/
    val filterRDD = primaryRDD
      .filter(line => line.split("\t")(4).contains("GET") | line.split("\t")(4).contains("POST"))
      .filter(line => line.split("\t")(1).split(",")(1).contains("www.bxd365.com")
        & (line.split("\t")(4).substring(4, line.split("\t")(4).lastIndexOf(" ")).trim.startsWith("/api").equals(false)
        | line.split("\t")(4).substring(4, line.split("\t")(4).lastIndexOf(" ")).trim.startsWith("/Api").equals(false)
        | line.split("\t")(4).substring(4, line.split("\t")(4).lastIndexOf(" ")).trim.startsWith("//api").equals(false)
        | line.split("\t")(4).substring(4, line.split("\t")(4).lastIndexOf(" ")).trim.startsWith("/CrmApi").equals(false))
        | line.split("\t")(1).split(",")(1).contains("rest-01.bxd365.com")
        | line.split("\t")(1).split(",")(1).contains("mall.bxd365.com")
        | line.split("\t")(1).split(",")(1).contains("bxd.9956.cn")
        | line.split("\t")(1).split(",")(1).contains("app.365kp.com")
        | line.split("\t")(1).split(",")(1).contains("*.bxd365.com")
        | line.split("\t")(1).split(",")(1).contains("huodong.bxd365.com"))
      .filter(line => line.split("\t")(4).contains(".jpg").equals(false)
        & line.split("\t")(4).contains(".png").equals(false)
        & line.split("\t")(4).contains(".gif").equals(false)
        & line.split("\t")(4).contains(".svg").equals(false)
        & line.split("\t")(4).contains(".css").equals(false)
        & line.split("\t")(4).contains(".js").equals(false)
        & line.split("\t")(4).contains("/ajax").equals(false)
        & line.split("\t")(4).contains("/nocache").equals(false)
        & line.split("\t")(4).contains("/regtb.html HTTP/").equals(false)
        & line.split("\t")(4).contains("/site/getsmscode").equals(false)
        & line.split("\t")(8).contains("Spider").equals(false)
        & line.split("\t")(8).contains("spider").equals(false))
      .filter(line => line.split("\t")(4).substring(4, line.split("\t")(4).lastIndexOf(" ")).trim.startsWith("/map")
        | line.split("\t")(4).substring(4, line.split("\t")(4).lastIndexOf(" ")).trim.startsWith("/m").equals(false)
        & line.split("\t")(4).substring(4, line.split("\t")(4).lastIndexOf(" ")).trim.startsWith("/vadmin").equals(false)
        & line.split("\t")(4).substring(4, line.split("\t")(4).lastIndexOf(" ")).trim.startsWith("/rest.php?").equals(false)
        & line.split("\t")(4).substring(4, line.split("\t")(4).lastIndexOf(" ")).trim.startsWith("/captcha").equals(false)
        & line.split("\t")(4).substring(4, line.split("\t")(4).lastIndexOf(" ")).trim.startsWith("/app/Apppng?").equals(false)
        & line.split("\t")(4).substring(4, line.split("\t")(4).lastIndexOf(" ")).trim.startsWith("/favicon.ico").equals(false)
        & line.split("\t")(4).substring(4, line.split("\t")(4).lastIndexOf(" ")).trim.startsWith("/query").equals(false)
        & line.split("\t")(4).substring(4, line.split("\t")(4).lastIndexOf(" ")).trim.startsWith("/site").equals(false)
        & line.split("\t")(4).substring(4, line.split("\t")(4).lastIndexOf(" ")).trim.startsWith("/static").equals(false))
      .filter(line => line.split("\t")(5).contains("200") | line.split("\t")(5).contains("301") |
        line.split("\t")(5).contains("302"))

    filterRDD.cache()

    /*取出ip和时间，并按照ip分组，按照时间升序排序*/
    val timeRDD = filterRDD.map(line => (line.split("\t")(0).split(",")(0), line.split("\t")(3)))
    val timestampRDD = timeRDD.map(line => (line._1, line._2.substring(line._2.lastIndexOf(":")-5,
      line._2.lastIndexOf(":")+3)))
    val groupRDD = timestampRDD.groupByKey()
      .sortByKey(true).map(x => (x._1, x._2.toList.sortWith(_<_)))
      .values.map(line => line.mkString(","))

    /*计算只进行一次访问的个数*/
    val num1 = groupRDD.filter(line => line.length == 8).count()
    /*计算多次访问的人数*/
    val num2 = groupRDD.filter(line => line.length > 8).count()

    val betweenRDD = groupRDD
      .filter(line => line.length>8)
      .map(line => line.split(","))
      .map(line => {
        val line1 = line.tail
        val result = line.zip(line1).map(p => {
          val df: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")
          val begin: Date = df.parse(p._1)
          val end: Date = df.parse(p._2)
          val between: Long = (end.getTime() - begin.getTime()) / 1000
          between
        })
        val real = result.filter(x => x.toInt>1800)
        real
      }).map(line => line.mkString(","))

    /*计算多次访问中的访问人次*/
    val num3 = betweenRDD.flatMap(x => x.split(",")).filter(y => y.length>0).count()

    val pv = filterRDD.count()
    val uv = filterRDD.map(line => (line.split("\t")(0).split(",")(0), line.split("\t")(8))).distinct().count()
    val iv = filterRDD.map(line => line.split("\t")(0).split(",")(0)).distinct().count()
    val outRDD = filterRDD.map(line => (line.split("\t")(0).split(",")(0)+line.split("\t")(8), 1))
      .reduceByKey{(a,b) => a+b}.values.filter(line=>line==1).count()

    //filterRDD.repartition(1).saveAsTextFile("hdfs://master1.hadoop:9000/prince/filter/20170425")
    //filterRDD.repartition(1).saveAsTextFile("E:\\filter")

    println("访问次数是：" + num1 + "+" + num2 + "+" + num3 + "=" + (num1+num2+num3) + "\n" + "访问量PV是：" + pv
      + "\n" + "独立访客数UV是：" + uv + "\n" + "独立IP数是：" + iv + "\n" + "跳出率是：" + outRDD/uv.toDouble)

    spark.stop()
  }
}
