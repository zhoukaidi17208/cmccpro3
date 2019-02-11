package cn.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UpdateStateByKeyDemo {
  def main(args:Array[String]){
    val conf =new SparkConf().setAppName("UpdateStateByKeyDemo")
    val ssc =new StreamingContext(conf,Seconds(20))
    //要使用updateStateByKey方法，必须设置Checkpoint。
    ssc.checkpoint("/checkpoint/")
    val socketLines = ssc.socketTextStream("localhost",9999)

    socketLines.flatMap(_.split(",")).map(word=>(word,1))
      .updateStateByKey( (currValues:Seq[Int],preValue:Option[Int])=>{
        val currValue = currValues.sum //将目前值相加
        Some(currValue + preValue.getOrElse(0)) //目前值的和加上历史值
      }).print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }

}
