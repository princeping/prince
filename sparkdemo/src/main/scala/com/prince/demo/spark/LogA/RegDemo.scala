package com.prince.demo.spark.LogA

import java.sql.Timestamp

import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * 计算注册量
  * 注册分为代理人注册和投保人注册，判断依据都是type字段为uid时判断为注册成功。
  * Created by princeping on 2017/4/12.
  */
object RegDemo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("RegDemo")
    val sc = new SparkContext(conf)
    val session = SparkSession.builder().getOrCreate()

    //val tbDF = session.read.json("E:\\LogA\\json\\20170414\\*_ebao2_Regtb.log")
    val dlDF = session.read.json("E:\\LogA\\json\\20170417\\*_ebao2_Reg.log")

    //tbDF.createOrReplaceTempView("tb_table")
    dlDF.createOrReplaceTempView("dl_table")

    //val regTBNum = session.sql("select type from tb_table where type>0 ")
    val regDLNum = session.sql("select type from dl_table where type>0 ")

    //val regTB_num = regTBNum.count()
    val regDL_num = regDLNum.count()

    //提取日志产出日期
    val dateDF = session.sql("select date from dl_table")
    val log_date = dateDF.rdd.map(_.toString().split(" ")(0).replace("[","")).first().trim

    //组装结果RDD
    //val arrayRDD = sc.parallelize(List((regTB_num,regDL_num,log_date)))
    val arrayRDD = sc.parallelize(List((regDL_num,log_date)))

    //将结果RDD映射到rowRDD
    val resultRowRDD = arrayRDD.map(p =>Row(
      //p._1.toInt,
      p._1.toInt,
      p._2.toString,
      new Timestamp(new java.util.Date().getTime)
    ))

    //通过StructType直接指定每个字段的schema
    val resultSchema = StructType(
      List(
        //tructField("regTB_num", IntegerType, true),
        StructField("regDL_num", IntegerType, true),
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
      .option("dbtable","reg")
      .option("user","root")
      .option("password","123456")
      .save()

    session.stop()
  }
}
