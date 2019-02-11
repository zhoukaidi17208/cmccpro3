package kafka

import java.util.Properties
import java.util.concurrent.Executors

import kafka.consumer.{Consumer, ConsumerConfig, KafkaStream}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

class ConsumerDemo(val consumer: String, val stream: KafkaStream[Array[Byte], Array[Byte]])
  extends Runnable{
  override def run() = {
    val it = stream.iterator()
    while (it.hasNext()) {
      val data = it.next()
      val offset = data.offset
      val partition = data.partition
      val topic = data.topic
      val msg = new String(data.message())

      println(s"Consumer: $consumer, Topic: $topic, " +
        s"Offset: $offset, msg: $msg")
    }
  }
}
object ConsumerDemo {
  def main(args: Array[String]): Unit = {
    // 定义需要读取的topic
    val topic = "JsonData"

    // 定义一个map，用于存储多个topic
    val topics = new mutable.HashMap[String, Int]()
    topics.put(topic, 2)

    // 创建配置信息类
    val props = new Properties()
    // 指定消费组
    props.put("group.id", "day1_001")
    // 指定zk的列表
    props.put("zookeeper.connect", "mini1:2181,mini2:2181,mini3:2181")
    // 如果zookeeper没有offset值或offset值超出范围，需要指定offset
    props.put("auto.offset.reset", "smallest")

    // 调用Condumer的配置对象
    val config = new ConsumerConfig(props)

    // 创建Consumer对象
    val consumer = Consumer.create(config)

    // 获取数据， key为：topic
    val streams: collection.Map[String, List[KafkaStream[Array[Byte], Array[Byte]]]] =
      consumer.createMessageStreams(topics)

    println(streams.keys)

    // 获取到指定topic的数据
    val stream: Option[List[KafkaStream[Array[Byte], Array[Byte]]]] = streams.get(topic)

    // 创建一个固定大小的线程池
    val pool = Executors.newFixedThreadPool(2)

    val sc = new SparkConf().setMaster("local[*]")
      .setAppName("实时统计")
      .set("spark.streaming.kafka.maxRatePerPartition", "50")


    val ssc = new StreamingContext(sc, Seconds(2))
    //获取kafka 数据，开始分析业务：






    for (i <- 0 until stream.size) {
      pool.execute(new ConsumerDemo(s"consumer: $i", stream.get(i)))

    }





  }

}