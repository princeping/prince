package com.prince.demo.kafka

import com.prince.demo.hbase.HBaseClient
import com.typesafe.config.ConfigFactory
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.hadoop.hbase.filter.PrefixFilter
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import redis.clients.jedis.JedisPool

import scala.util.parsing.json.JSONObject

/**
  * Created by princeping on 2017/7/19.
  */
object KafkaWriteToRedis {
  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {

    implicit val conf = ConfigFactory.load

    val spark = SparkSession.builder.appName("KafkaWriteToKafka").master("local[*]").getOrCreate()

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
      .createDirectStream(ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams)).map(_.value)

    stream.foreachRDD(rdd => {
      rdd.foreach(println)
    })

    stream.foreachRDD(rdd => {
      val df = spark.read.json(rdd)
      df.foreachPartition(partition => {
        partition.foreach(x => {
          val spark2 = SparkSession.builder.getOrCreate()
          val key = x.getString(0)
          val filter1 = x.getString(1)
          val filter2 = x.getString(2)
          val startTime = x.getString(3)
          val stopTime = x.getString(4)
          val out = funnelDemo1(spark2, filter1, filter2, startTime, stopTime)
          val array = (key, out._1, out._2)
          object InternalRedisClient extends Serializable {

            @transient private var pool:JedisPool = null

            def makePool(redisHost: String, redisPort: Int, redisTimeout: Int,
                         maxTotal: Int, maxIdle: Int, minIdle: Int): Unit = {
              makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle, true, false, 10000)
            }
            def makePool(redisHost: String, redisPort: Int, redisTimeout: Int,
                         maxTotal: Int, maxIdle: Int, minIdle: Int, testOnBorrow: Boolean,
                         testOnReturn: Boolean, maxWaitMillis: Long): Unit ={
              if (pool == null) {
                val poolConfig = new GenericObjectPoolConfig
                poolConfig.setMaxTotal(maxTotal)
                poolConfig.setMaxIdle(maxIdle)
                poolConfig.setMinIdle(minIdle)
                poolConfig.setTestOnBorrow(testOnBorrow)
                poolConfig.setTestOnReturn(testOnReturn)
                poolConfig.setMaxWaitMillis(maxWaitMillis)
                pool = new JedisPool(poolConfig, redisHost, redisPort, redisTimeout)

                val hook = new Thread {
                  override def run() = pool.destroy()
                }
                sys.addShutdownHook(hook.run)
              }
            }

            def getPool: JedisPool = {
              assert(pool != null)
              pool
            }
          }
          val maxTotal = 10
          val maxIdle = 10
          val minIdle = 1
          val redisHost = "192.168.1.24"
          val redisPort = 6379
          val redisTimeout = 30000
          val dbIndex = 1
          InternalRedisClient.makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle)
          val num1 = array._2.toString
          val num2 = array._3.toString
          val jedis = InternalRedisClient.getPool.getResource
          jedis.select(dbIndex)

          jedis.lpush(array._1, num1, num2)
          InternalRedisClient.getPool.returnResource(jedis)
        })
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
  /**
    * 两项圈选
    * @param filter1 圈选1
    * @param filter2 圈选2
    * @param startTime 起始时间
    * @param stopTime 截止时间
    * @return
    */
  def funnelDemo1(spark: SparkSession, filter1: String, filter2: String, startTime: String, stopTime: String): (Int, Int) = {
    val startRow1 = filter1 + "_" + startTime
    val stopRow1 = filter1 + "_" + stopTime
    val startRow2 = filter2 + "_" + startTime
    val stopRow2 = filter2 + "_" + stopTime
    val df1 = getDF(spark, filter1, startRow1, stopRow1)
    val df2 = getDF(spark, filter2, startRow2, stopRow2)
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
    * 组装json
    * @param filter 圈选条件
    * @param startRow 起始行
    * @param stopRow 截止行
    * @return
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
}