package com.prince.demo.spark.LogA

import java.sql.Timestamp

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/**
  * 计算提问量以及提问人数
  * 计算规则：url中含有/qa/add..，并且params值不为空，由于本例中params对应的value含有"[]"，
  * 因此判断params时应判断其长度>2
  * Created by princeping on 2017/4/12.
  */
object QaDemo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("QaDemo")
    val sc = new SparkContext(conf)
    val session = SparkSession.builder().getOrCreate()

    val userDF = session.read.json("E:\\LogA\\json\\20170417\\*_ebao2_qa.log")
    userDF.createOrReplaceTempView("tmp_table")

    val qaDF = session.sql("select server_data.url,server_data.ip,params from tmp_table where length(params)>2")

    val addRDD1 = qaDF.rdd.map(line=>line.mkString("\t")).filter(x=>x.startsWith("www.bxd365.com/qa/add")).distinct()
    val ansRDD1 = qaDF.rdd.map(line=>line.mkString("\t")).filter(x=>x.startsWith("www.bxd365.com/qa/answer")).distinct()

    val addRDD2 = addRDD1.map(line => (line.split("\t")(1),line)).reduceByKey{(a,b) => a+b}
    val ansRDD2 = ansRDD1.map(line => (line.split("\t")(1),line)).reduceByKey{(a,b) => a+b}

    val add_num = addRDD1.count()
    val add_people_num = addRDD2.count()
    val ans_num = ansRDD1.count()
    val ans_people_num = ansRDD2.count()

    //提取日志产出日期
    val dateDF = session.sql("select date from tmp_table")
    val log_date = dateDF.rdd.map(_.toString().split(" ")(0).replace("[","")).first().trim

    //组装结果RDD
    val arrayRDD = sc.parallelize(List((add_num,add_people_num,ans_num,ans_people_num,log_date)))

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
        StructField("add_num", IntegerType, true),
        StructField("add_people_num", IntegerType, true),
        StructField("ans_num", IntegerType, true),
        StructField("ans_people_num", IntegerType, true),
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
      .option("dbtable","qa")
      .option("user","root")
      .option("password","123456")
      .save()

    session.stop()
  }
}
