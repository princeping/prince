package com.prince.demo.spark.nginx

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.SparkSession

/**
  * Created by princeping on 2017/5/11.
  */
object NginxDemoFour {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("NginxDemoFour").getOrCreate()
    //val spark = SparkSession.builder().master("spark://master1.hadoop:7077").appName("NginxDemoFour").getOrCreate()

    //val input = spark.sparkContext.textFile("E:\\LogA\\nginx\\data\\log\\nginx\\3w.log")
    val input = spark.sparkContext.textFile("E:\\LogA\\nginx\\data\\log\\nginx\\access_20170502_*.log")
    //val input = spark.sparkContext.textFile("hdfs://master1.hadoop:9000/prince/data/20170428/access_20170430_*.log")

    val primaryRDD = input
//      .filter(line => line.startsWith("\""))//实测有些脏数据不是引号开头
      .map(line => line.substring(line.indexOf("\""), line.lastIndexOf("\"")+1))
      .map(line => line.replace("\"", ""))
      .map(line => line.replace(", ", ","))

    /*过滤出有效日志*/
    val filterRDD = primaryRDD
      .filter(line => line.split("\\|")(6).contains("GET") | line.split("\\|")(6).contains("POST"))
      .filter(line => line.split("\\|")(7).contains("200") | line.split("\\|")(7).contains("301") |
        line.split("\\|")(7).contains("302"))
      .filter(line => line.split("\\|")(6).contains(".jpg").equals(false)
        & line.split("\\|")(6).contains(".jpeg").equals(false)
        & line.split("\\|")(6).contains(".png").equals(false)
        & line.split("\\|")(6).contains(".gif").equals(false)
        & line.split("\\|")(6).contains(".svg").equals(false)
        & line.split("\\|")(6).contains(".css").equals(false)
        & line.split("\\|")(6).contains(".js").equals(false)
        & line.split("\\|")(6).contains("/ajax").equals(false)
        & line.split("\\|")(6).contains("/nocache").equals(false)
        & line.split("\\|")(6).contains("/regtb.html HTTP/").equals(false)
        & line.split("\\|")(6).contains("/site/getsmscode").equals(false)
        & line.split("\\|")(6).contains("/robots.txt").equals(false)
        & line.split("\\|")(10).contains("Spider").equals(false)
        & line.split("\\|")(10).contains("spider").equals(false)
        & line.split("\\|")(10).contains("robot").equals(false)
        & line.split("\\|")(10).contains("bot").equals(false)
        & line.split("\\|")(10).contains("Bot").equals(false))
      .filter(line => line.split("\\|")(3).contains("mall.bxd365.com")
        | line.split("\\|")(3).contains("bxd.9956.cn")
        | line.split("\\|")(3).contains("app.365kp.com")
        | line.split("\\|")(3).contains("*.bxd365.com")
        | line.split("\\|")(3).contains("huodong.bxd365.com")
        |(line.split("\\|")(3).contains("rest-01.bxd365.com")
        & line.split("\\|")(6).contains("/pushready").equals(false)
        & line.split("\\|")(6).contains("/api/forward/add").equals(false)
        & line.split("\\|")(6).contains("/check_version").equals(false)
        & line.split("\\|")(6).contains("/api/commonIntface/newMessage").equals(false)
        & line.split("\\|")(6).contains("/api/commonIntface/actionTrigger").equals(false)
        & line.split("\\|")(6).contains("/api/jokes/listtop").equals(false)
        & line.split("\\|")(6).contains("/api/index/navigation").equals(false)
        & line.split("\\|")(6).contains("/getcommonkeywords").equals(false))
        |(line.split("\\|")(3).contains("www.bxd365.com")
        & line.split("\\|")(6).substring(4, line.split("\\|")(6).lastIndexOf(".")-6).trim.startsWith("/api").equals(false)
        & line.split("\\|")(6).substring(4, line.split("\\|")(6).lastIndexOf(".")-6).trim.startsWith("/Api").equals(false)
        & line.split("\\|")(6).substring(4, line.split("\\|")(6).lastIndexOf(".")-6).trim.startsWith("//api").equals(false)
        & line.split("\\|")(6).substring(4, line.split("\\|")(6).lastIndexOf(".")-6).trim.startsWith("/CrmApi").equals(false)
        & line.split("\\|")(6).substring(4, line.split("\\|")(6).lastIndexOf(".")-6).trim.startsWith("/vadmin").equals(false)
        & line.split("\\|")(6).substring(4, line.split("\\|")(6).lastIndexOf(".")-6).trim.startsWith("/rest.php?").equals(false)
        & line.split("\\|")(6).substring(4, line.split("\\|")(6).lastIndexOf(".")-6).trim.startsWith("/captcha").equals(false)
        & line.split("\\|")(6).substring(4, line.split("\\|")(6).lastIndexOf(".")-6).trim.startsWith("/app/Apppng?").equals(false)
        & line.split("\\|")(6).substring(4, line.split("\\|")(6).lastIndexOf(".")-6).trim.startsWith("/favicon.ico").equals(false)
        & line.split("\\|")(6).substring(4, line.split("\\|")(6).lastIndexOf(".")-6).trim.startsWith("/query").equals(false)
        & line.split("\\|")(6).substring(4, line.split("\\|")(6).lastIndexOf(".")-6).trim.startsWith("/site").equals(false)
        & line.split("\\|")(6).substring(4, line.split("\\|")(6).lastIndexOf(".")-6).trim.startsWith("/static").equals(false)
        & line.split("\\|")(6).substring(4, line.split("\\|")(6).lastIndexOf(".")-6).trim.startsWith("/minsurers").equals(false)))
      .filter(line => line.split("\\|")(6).substring(4, line.split("\\|")(6).lastIndexOf(".")-6).trim.startsWith("/map")
        | line.split("\\|")(6).substring(4, line.split("\\|")(6).lastIndexOf(".")-6).trim.startsWith("/m").equals(false))

    //cache到内存，提高计算效率
    filterRDD.cache()

    /*取出ip和时间，并按照ip分组，按照时间升序排序*/
    val timeRDD = filterRDD.map(line => (line.split("\\|")(0).split(",")(0), line.split("\\|")(5)))
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
    val uv = filterRDD.map(line => (line.split("\\|")(0).split(",")(0), line.split("\\|")(10))).distinct().count()
    val iv = filterRDD.map(line => line.split("\\|")(0).split(",")(0)).distinct().count()
    val outRDD = filterRDD.map(line => (line.split("\\|")(0).split(",")(0)+line.split("\\|")(10), 1))
      .reduceByKey{(a,b) => a+b}.values.filter(line=>line==1).count()

    //filterRDD.repartition(1).saveAsTextFile("hdfs://master1.hadoop:9000/prince/filter/20170430")
    filterRDD.repartition(1).saveAsTextFile("E:\\filter\\502")

    println("访问次数是：" + num1 + "+" + num2 + "+" + num3 + "=" + (num1+num2+num3) + "\n" + "访问量PV是：" + pv
      + "\n" + "独立访客数UV是：" + uv + "\n" + "独立IP数是：" + iv + "\n" + "跳出率是：" + outRDD/uv.toDouble)

    spark.stop()
  }
}
