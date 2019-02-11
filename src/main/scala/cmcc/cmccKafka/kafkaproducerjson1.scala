package cmcc.cmccKafka

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import scala.io.Source

object kafkaproducerjson1 {
  def main(args: Array[String]): Unit = {
   val topic="JsonData"

    val props=new Properties()
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("metadata.broker.list", "mini1:9092,mini2:9092,mini3:9092")
    props.put("request.required.acks", "1")
    props.put("partitioner.class", "cn.kafka.CustomPartitioner")
    // 把配置信息封装到ProducerConfig中
    val config = new ProducerConfig(props)

    // 创建Producer实例
    val producer: Producer[String, String] = new Producer(config)

    var source: Iterator[String] = Source.fromFile("d:/cmcc.json").getLines()

    for (elem <- source) {
      producer.send(new KeyedMessage[String,String](topic,elem.toString))
      println(elem.toString)
      Thread.sleep(5)

    }
    producer.close()
    println("已经发送完毕。。。。。。。。。。。。")

  }

}
