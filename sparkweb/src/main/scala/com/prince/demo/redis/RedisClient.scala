package com.prince.demo.redis

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

/**
  * Created by princeping on 2017/8/21.
  */
object RedisClient extends Serializable{
  val redisHost = "192.168.1.24"
  val redisPort = 6379
  val redisTimeout = 3000
  lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout)

  lazy val hook = new Thread {
    override def run = {
      println("Execute hook thread:" + this)
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook.run)
}
