package cn.kafka

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool


object RedisClient {
  /**
  * 因为数据要存入redis中，获取redis客户端代码如下
  */
  val redisHost = "127.0.0.1"
  val redisPort = 6379
  val redisTimeout = 30000

  lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout)
  lazy val hook = new Thread {
    override def run = {
      println("Execute hook thread: " + this)
      pool.destroy()
    }
  }

  sys.addShutdownHook(hook.run)

}
