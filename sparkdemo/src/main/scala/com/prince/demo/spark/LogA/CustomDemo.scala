package com.prince.demo.spark.LogA

import java.sql.Timestamp

import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * 计算订制量
  * url中含有addcustom，且data字段不为空的表示订制成功。按照ip去重后得到订制人数
  * Created by princeping on 2017/4/14.
  */
object CustomDemo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("CustomDemo")
    val sc = new SparkContext(conf)
    val session = SparkSession.builder().getOrCreate()

    val customDF = session.read.json("E:\\LogA\\json\\20170417\\*_ebao2_Custom.log")

    customDF.createOrReplaceTempView("tmp_table")

    val orderDF = session.sql("select server_data.url,server_data.ip,data  from tmp_table where server_data.url =" +
      " 'www.bxd365.com/nproduct/addcustom'")

    val customRDD1 = orderDF.rdd.distinct().map(line => line.mkString("\t"))
    val customRDD2 = customRDD1.map(line => (line.split("\t")(1),line)).reduceByKey{(a,b) => a+b}

    val custom_num = customRDD1.count()
    val custom_people_num = customRDD2.count()

    //提取日志产出日期
    val dateDF = session.sql("select date from tmp_table")
    val log_date = dateDF.rdd.map(_.toString().split(" ")(0).replace("[","")).first().trim

    //组装结果RDD
    val arrayRDD = sc.parallelize(List((custom_num,custom_people_num,log_date)))

    //将结果RDD映射到rowRDD
    val resultRowRDD = arrayRDD.map(p =>Row(
      p._1.toInt,
      p._2.toInt,
      p._3.toString,
      new Timestamp(new java.util.Date().getTime)
    ))

    //通过StructType直接指定每个字段的schema
    val resultSchema = StructType(
      List(
        StructField("custom_num", IntegerType, true),
        StructField("custom_people_num", IntegerType, true),
        StructField("log_date", StringType, true),//是哪一天日志分析出来的结果
        StructField("create_time", TimestampType, true)//分析结果的创建时间
      )
    )

    //组装新的DataFrame
    val DF = session.createDataFrame(resultRowRDD,resultSchema)

    //将结果写入到Mysql
    DF.write.mode("append")
      .format("jdbc")
      .option("url","jdbc:mysql://192.168.1.97:3306/xiangju_log")
      .option("dbtable","custom")
      .option("user","root")
      .option("password","123456")
      .save()

    session.stop()
  }
}
