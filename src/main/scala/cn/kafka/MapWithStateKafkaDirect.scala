//package cn.kafka
//
//import kafka.serializer.StringDecoder
//import org.apache.spark.streaming.dstream.DStream
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
//import org.apache.spark.{SparkConf, SparkContext}
//
//object MapWithStateKafkaDirect {
//  def main(args: Array[String]): Unit = {
//
//
//    val conf = new SparkConf().setAppName("MapWithStateKafkaDirect").setMaster("local[*]")
//    val sc = SparkContext.getOrCreate(conf)
//    val checkpointDir = "hdfs://hdfs-cluster/ecommerce/checkpoint/2"
//    val brokers = "hadoop-all-01:9092,hadoop-all-02:9092,hadoop-all-03:9092"
//
//
//    def mappingFunction(key: String, value: Option[Int], state: State[Long]): (String, Long) = {
//      // 获取之前状态的值
//      val oldState = state.getOption().getOrElse(0L)
//      // 计算当前状态值
//      val newState = oldState + value.getOrElse(0)
//      // 更新状态值
//      state.update(newState)
//      // 返回结果
//      (key, newState)
//    }
//
//    def creatingFunc(): StreamingContext = {
//      val ssc = new StreamingContext(sc, Seconds(5))
//      val kafkaParams = Map(
//        "metadata.broker.list" -> brokers,
//        "group.id" -> "MapWithStateKafkaDirect"
//      )
//      val spec = StateSpec.function[String, Int, Long,(String,Int)]
//
//      val topics = Set("count")
//      val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
//      val results: DStream[(String, Long)] = messages
//        .filter(_._2.nonEmpty)
//        .mapPartitions(iter => {
//          iter.flatMap(_._2.split(" ").map((_, 1)))
//        })
//        .mapWithState(spec)
//      results.print()
//      ssc.checkpoint(checkpointDir)
//      ssc
//    }
//
//
//    def close(ssc: StreamingContext, millis: Long): Unit = {
//      new Thread(new Runnable {
//        override def run(): Unit = {
//          // 当某个条件触发而关闭StreamingContext
//          Thread.sleep(millis)
//          println("满足条件,准备关闭StreamingContext")
//          ssc.stop(true, true)
//          println("成功关闭StreamingContext")
//        }
//      }).start()
//    }
//
//    val ssc = StreamingContext.getOrCreate(checkpointDir, creatingFunc)
//
//    ssc.start()
//    ssc.awaitTermination()
//    close(ssc, 20000)
//
//  }
//}