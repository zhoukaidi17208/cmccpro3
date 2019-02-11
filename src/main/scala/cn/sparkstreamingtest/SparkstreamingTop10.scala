package cn.sparkstreamingtest

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkstreamingTop10 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Sparkstreamingtop10").setMaster("local")
    var ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    val words: ReceiverInputDStream[String] = ssc.socketTextStream("mini1",9999)

    words.foreachRDD(rdd=>{

    })


  }

}
