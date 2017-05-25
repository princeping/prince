package com.prince.demo.spark.LogA

import java.sql.Timestamp

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/**
  * 计算认证量
  * url中含有www.bxd365.com/cagent2/verify，verify字段含有0和1两种值
  * Created by princeping on 2017/4/12.
  */
object VerifyDemo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("VerifyDemo")
    val sc = new SparkContext(conf)
    val session = SparkSession.builder().getOrCreate()


    val userDF = session.read.json("E:\\LogA\\json\\20170417\\*_ebao2_verify.log")
    userDF.createOrReplaceTempView("tmp_table")

    val verifyDF = session.sql("select server_data.ip,params.uagent.ua_is_verify from tmp_table where " +
      "server_data.url = 'www.bxd365.com/cagent2/verify'")

    val rdd1 = verifyDF.rdd.distinct().map(line => line.mkString("\t"))
    val rdd2 = rdd1.map(line => (line.split("\t")(0),line.split("\t")(1))).reduceByKey{(a,b) => a+b}
    val rdd3 = rdd2.filter(line => line._2.length>1).count()

    //提取日志产出日期
    val dateDF = session.sql("select date from tmp_table")
    val log_date = dateDF.rdd.map(_.toString().split(" ")(0).replace("[","")).first().trim

    //组装结果RDD
    val arrayRDD = sc.parallelize(List((rdd3,log_date)))

    //将结果RDD映射到rowRDD
    val resultRowRDD = arrayRDD.map(p =>Row(
      p._1.toInt,
      p._2.toString,
      new Timestamp(new java.util.Date().getTime)
    ))

    //通过StructType直接指定每个字段的schema
    val resultSchema = StructType(
      List(
        StructField("verify_num", IntegerType, true),
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
      .option("dbtable","verify")
      .option("user","root")
      .option("password","123456")
      .save()

    session.stop()
  }
}
