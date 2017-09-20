package com.prince.demo.kafka

import java.util.Properties

import com.typesafe.config.ConfigFactory
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.log4j.{Level, Logger}
import org.codehaus.jettison.json.JSONObject

import scala.util.Random

/**
  * Created by princeping on 2017/8/17.
  */
object KafkaProducer {

  Logger.getLogger("org").setLevel(Level.WARN)
  private val users = Array(
    "b2e4a1bb-741b-4700-8b03-18c06a280", "b2e4a1bb-741b-4700-8b03-18c06a248",
    "b2e4a1bb-741b-4700-8b03-18c06a268", "b2e4a1bb-741b-4700-8b03-18c06a212",
    "b2e4a1bb-741b-4700-8b03-18c06a266", "b2e4a1bb-741b-4700-8b03-18c06a258",
    "b2e4a1bb-741b-4700-8b03-18c06a232", "b2e4a1bb-741b-4700-8b03-18c06a224",
    "b2e4a1bb-741b-4700-8b03-18c06a298", "b2e4a1bb-741b-4700-8b03-18c06a275"
  )

  private val tokens = Array(
    "v2ff37ca54eda70e0c1b8902626cb6dd", "fb751fb989ce159e3ee5149927176474",
    "af89557c629d6b7af43378df4b8f30d9", "n3f164f9e9999eefa13064ac1e864fd8",
    "zbd6f5791a99249c3a513b21ce835038", "dc6470493c3c891db6f63326b19ef482",
    "k2917b1b391186ff8f032f4326778ef7", "ca796f74ee74360e169fc290f1e720c7",
    "h981bd53a475b4edc9b0ad5f72870b03", "p4064d445c9f4ff4d536dfeae965aa95"
  )

  private val random = new Random()

  def point(): Int = {
    random.nextInt(10)
  }

  def getUserID(): String = {
    users(point())
  }

  def getToken(): String = {
    tokens(point())
  }

  def main(args: Array[String]): Unit = {
    implicit val conf = ConfigFactory.load
    val topic = "topic009"
    val brokers = conf.getString("kafka.brokers")
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")

    val kafkaConfig = new ProducerConfig(props)
    val producer = new Producer[String, String](kafkaConfig)

    while (true) {
      val event = new JSONObject()
      event
        .put("ju_userid", getUserID())
        .put("qurey_time", System.currentTimeMillis.toString)
        .put("token", getToken())
      producer.send(new KeyedMessage[String, String](topic, "key", event.toString))
      println("Message sent: " + event)

      Thread.sleep(1000)
    }
  }
}
