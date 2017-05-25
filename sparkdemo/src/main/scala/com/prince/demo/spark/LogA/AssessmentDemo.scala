package com.prince.demo.spark.LogA

import java.sql.Timestamp

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

/**
  * 计算测评量
  * 计算规则：url后缀以start结尾表示进入测评页面，以result结尾表示成功完成了测评
  * 根据ip排重后可以进一步得到两种状态下的测评人数
  * Created by princeping on 2017/4/12.
  */
object AssessmentDemo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("AssessmentDemo")
    val sc = new SparkContext(conf)
    val session = SparkSession.builder().getOrCreate()

    val userDF = session.read.json("E:\\LogA\\json\\20170417\\*_ebao2_Newsessment.log")
    userDF.createOrReplaceTempView("tmp_table")

    val startDF = session.sql("select  server_data.ip from tmp_table where server_data.url =" +
      " 'www.bxd365.com/newsessment/start'")
    val resultDF = session.sql("select server_data.ip from tmp_table where server_data.url =" +
      " 'www.bxd365.com/newsessment/result'")

    val startRDD = startDF.rdd.distinct()
    val resultRDD = resultDF.rdd.distinct()

    val start_num = startDF.count()
    val start_people_num = startRDD.count()
    val result_num = resultDF.count()
    val result_people_num = resultRDD.count()

    //提取日志产出日期
    val dateDF = session.sql("select date from tmp_table")
    val log_date = dateDF.rdd.map(_.toString().split(" ")(0).replace("[","")).first().trim

    //组装结果RDD
    val arrayRDD = sc.parallelize(List((start_num,start_people_num,result_num,result_people_num,log_date)))

    //将结果RDD映射到rowRDD
    val resultRowRDD = arrayRDD.map(p =>Row(
      p._1.toInt,
      p._2.toInt,
      p._3.toInt,
      p._4.toInt,
      p._5.toString,
      new Timestamp(new java.util.Date().getTime)
    ))

    //通过StructType直接指定每个字段的schema
    val resultSchema = StructType(
      List(
        StructField("start_num", IntegerType, true),
        StructField("start_people_num", IntegerType, true),
        StructField("result_num", IntegerType, true),
        StructField("result_people_num", IntegerType, true),
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
      .option("dbtable","assessment")
      .option("user","root")
      .option("password","123456")
      .save()

    session.stop()
  }
}
