package com.prince.demo.kafka

import java.util.Properties

import com.prince.demo.hbase.HBaseClient
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.hbase.filter.PrefixFilter
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.github.benfradet.spark.kafka010.writer._
import org.apache.kafka.clients.producer.ProducerRecord

import scala.util.parsing.json.JSONObject

/**
  * Created by princeping on 2017/9/20.
  */
object KafkaWriteKafka {

  Logger.getLogger("org").setLevel(Level.WARN)
  implicit val conf: Config = ConfigFactory.load

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("KafkaWriteRedis").master("local[*]").getOrCreate()
    val sparkContext = spark.sparkContext
    val ssc = new StreamingContext(sparkContext, Seconds(1))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> conf.getString("kafka.brokers"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> conf.getString("kafka.group"),
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val topic = conf.getString("kafka.topics")
    val topics = Array(topic)
    val stream = KafkaUtils
      .createDirectStream(ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    val input = stream.flatMap(line => {
      Some(line.value.toString)
    })

    input.foreachRDD(rdd => {
      if (!rdd.isEmpty) {
        val df = spark.read.json(rdd)
        df.foreachPartition(part => {
          part.foreach(x => {
            val spark2 = SparkSession.builder.getOrCreate()
            if (x.getString(0).equals("circle")) {
              if (x.length == 5){//两项圈选
                val out = funnelDemo2(spark2, x.getString(1), x.getString(2), x.getString(3), x.getString(4))
                submitToKafka(spark2, out.toString)
              }else if (x.length == 6){//三项圈选;
                val out = funnelDemo3(spark2, x.getString(1), x.getString(2), x.getString(3), x.getString(4), x.getString(5))
                submitToKafka(spark2, out.toString)
              }
            }else {
              println("-------------")
            }
          })
        })
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
  /**
    * 两项圈选
    * @param filter1 token1
    * @param filter2 token2
    * @param startTime 起始时间
    * @param stopTime 截止时间
    */
  def funnelDemo2(spark: SparkSession, filter1: String, filter2: String, startTime: String,
                  stopTime: String): (Int, Int) = {
    val df1 = getDF(spark, filter1, filter1 + "_" + startTime, filter1 + "_" + stopTime)
    val df2 = getDF(spark, filter2, filter2 + "_" + startTime, filter2 + "_" + stopTime)
    val num1 = df1.select("*").count().toInt
    val num2 = df2.select("*").count().toInt
    if (num1 != 0 & num2 != 0) {
      df1.createOrReplaceTempView("temp1")
      df2.createOrReplaceTempView("temp2")
      val rdd1 = spark.sql("select id from temp1").rdd
      val rdd2 = spark.sql("select id from temp2").rdd
      val count = rdd1.intersection(rdd2).count().toInt
      (rdd1.count().toInt, count)
    } else {
      println("两个token至少其中一个在这段时间内访问量为0!")
      (num1, num2)
    }
  }

  /**
    * 三项圈选
    * @param filter1 token1
    * @param filter2 token2
    * @param filter3 token3
    * @param startTime 起始时间
    * @param stopTime 截止时间
    */
  def funnelDemo3(spark: SparkSession, filter1: String, filter2: String, filter3: String,
                  startTime: String, stopTime: String): (Int, Int, Int) = {
    val df1 = getDF(spark, filter1, filter1+"_"+startTime, filter1+"_"+stopTime)
    val df2 = getDF(spark, filter2, filter2+"_"+startTime, filter2+"_"+stopTime)
    val df3 = getDF(spark, filter3, filter3+"_"+startTime, filter3+"_"+stopTime)
    val num1 = df1.select("*").count().toInt
    val num2 = df2.select("*").count().toInt
    val num3 = df3.select("*").count().toInt
    if (num1 != 0 & num2 != 0 & num3 != 0) {
      df1.createOrReplaceTempView("temp1")
      df2.createOrReplaceTempView("temp2")
      df3.createOrReplaceTempView("temp3")
      val rdd1 = spark.sql("select id from temp1").rdd
      val rdd2 = spark.sql("select id from temp2").rdd
      val rdd3 = spark.sql("select id from temp3").rdd
      val count1 = rdd1.intersection(rdd2)
      val count2 = count1.intersection(rdd3).count().toInt
      (rdd1.count().toInt, count1.count().toInt, count2)
    }else {
      println("三个token中至少一个在这位短时间内访问量为0!")
      (num1, num2, num3)
    }
  }

  /**
    * 组装json
    * @param filter 圈选条件
    * @param startRow 起始行
    * @param stopRow 截止行
    */
  def getDF(spark: SparkSession, filter: String, startRow: String, stopRow: String): DataFrame = {
    val filter1 = new PrefixFilter(Bytes.toBytes(filter))
    val results = HBaseClient.scan("circle", filter1, startRow, stopRow)
    val jsonString = results.map(e => JSONObject(e).toString())
    val jsonRDD = spark.sparkContext.parallelize(jsonString)
    val df = spark.read.json(jsonRDD)
    df.show()
    df
  }

  /**
    * 将计算结果提交到kafka
    * @param spark SparkSession
    * @param input 计算结果
    */
  def submitToKafka(spark:SparkSession, input:String): Unit ={
    val topic = "topic009"
    val producerConfig = {
      val p = new Properties()
      p.setProperty("bootstrap.servers", conf.getString("kafka.brokers"))
      p.setProperty("key.serializer", classOf[StringSerializer].getName)
      p.setProperty("value.serializer", classOf[StringSerializer].getName)
      p
    }
    val test = spark.sparkContext.makeRDD(Array(input))
    test.writeToKafka(
      producerConfig,
      s => new ProducerRecord[String, String](topic, s)
    )
  }
}
