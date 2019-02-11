package cn.sheep.cmccpro

import cn.sheep.utils.{JedisUtils, Utils}
import com.alibaba.fastjson.JSON
import com.typesafe.config.ConfigFactory
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc._
import scalikejdbc.config.DBs

object RTMonitor {

  // 屏蔽日志
  Logger.getLogger("org.apache").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {

    val load = ConfigFactory.load()

    // 创建kafka相关参数
    val kafkaParams = Map(
      "metadata.broker.list" -> load.getString("kafka.broker.list"),
      "group.id" -> load.getString("kafka.group.id"),
      "auto.offset.reset" -> "smallest"
    )
    val topics = load.getString("kafka.topics").split(",").toSet

    // StreamingContext
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[*]")
    sparkConf.setAppName("实时统计")
    val ssc = new StreamingContext(sparkConf, Seconds(2)) // 批次时间应该大于这个批次处理完的总的花费（total delay）时间

    // 从kafka弄数据 --- 从数据库中获取到当前的消费到的偏移量位置 -- 从该位置接着往后消费

    // 加载配置信息
    DBs.setup()
    val fromOffsets: Map[TopicAndPartition, Long] = DB.readOnly{implicit session =>
      sql"select * from streaming_offset_1 where groupid=?".bind(load.getString("kafka.group.id")).map(rs => {
        (TopicAndPartition(rs.string("topic"), rs.int("partitions")), rs.long("offset"))
      }).list().apply()
    }.toMap


    val stream = if (fromOffsets.size == 0) { // 假设程序第一次启动
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    } else {
      var checkedOffset = Map[TopicAndPartition, Long]()
      val kafkaCluster = new KafkaCluster(kafkaParams)
      val earliestLeaderOffsets = kafkaCluster.getEarliestLeaderOffsets(fromOffsets.keySet)
      if (earliestLeaderOffsets.isRight) {
        val topicAndPartitionToOffset = earliestLeaderOffsets.right.get

        // 开始对比
        checkedOffset = fromOffsets.map(owner => {
          val clusterEarliestOffset = topicAndPartitionToOffset.get(owner._1).get.offset
          if (owner._2 >= clusterEarliestOffset) {
            owner
          } else {
            (owner._1, clusterEarliestOffset)
          }
        })
      }
      // 程序菲第一次启动
      val messageHandler = (mm: MessageAndMetadata[String, String]) => (mm.key(), mm.message())
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, checkedOffset, messageHandler)
    }

    /**
      * receiver 接受数据是在Executor端 cache -- 如果使用的窗口函数的话，没必要进行cache, 默认就是cache， WAL ；
      *                                       如果采用的不是窗口函数操作的话，你可以cache, 数据会放做一个副本放到另外一台节点上做容错
      * direct 接受数据是在Driver端
      */
    // 处理数据 ---- 根据业务--需求
    stream.foreachRDD(rdd => {
      // rdd.foreach(println)
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges


      val baseData = rdd.map(t => JSON.parseObject(t._2))
        .filter(_.getString("serviceName").equalsIgnoreCase("reChargeNotifyReq"))
        .map(jsObj => {

          val result = jsObj.getString("bussinessRst")
          val fee: Double = if (result.equals("0000")) jsObj.getDouble("chargefee") else 0
          val isSucc: Double = if (result.equals("0000")) 1 else 0

          val receiveTime = jsObj.getString("receiveNotifyTime")
          val startTime = jsObj.getString("requestId")

          val pCode = jsObj.getString("provinceCode")

          // 消耗时间
          val costime = if (result.equals("0000")) Utils.caculateRqt(startTime, receiveTime) else 0

          ("A-" + startTime.substring(0, 8), startTime.substring(0, 10), List[Double](1, isSucc, fee, costime.toDouble), pCode, startTime.substring(0, 12))
        })


      // 实时报表 -- 业务概况
      /**
        * 1)统计全网的充值订单量, 充值金额, 充值成功率及充值平均时长.
        */
      baseData.map(t => ("total"+t._1, t._3)).reduceByKey((list1, list2) => {
        (list1 zip list2) map(x => x._1 + x._2)
      }).foreachPartition(itr => {

        val client = JedisUtils.getJedisClient()

        itr.foreach(tp => {
          client.set(tp._1,  tp._2(0).toString)
          client.set(tp._1, tp._2(1).toString)
          client.set(tp._1,  tp._2(2).toString)
          client.set(tp._1,  tp._2(3).toString)

          client.expire(tp._1, 60 * 60 * 24 * 2)
        })
        client.close()
      })

      // 每个小时的数据分布情况统计
      baseData.map(t => ("B-"+t._2, t._3)).reduceByKey((list1, list2) => {
        (list1 zip list2) map(x => x._1 + x._2)
      }).foreachPartition(itr => {

        val client = JedisUtils.getJedisClient()

        itr.foreach(tp => {
          // B-2017111816
          client.set("everyhour_succ"+tp._1, tp._2(0).toString)
          client.set("everyhour_succmoney"+tp._1, tp._2(1).toString)

          client.expire(tp._1, 60 * 60 * 24 * 2)
        })
        client.close()
      })



      // 每个省份充值成功数据
      baseData.map(t => ((t._2, t._4), t._3)).reduceByKey((list1, list2) => {
        (list1 zip list2) map(x => x._1 + x._2)
      }).foreachPartition(itr => {

        val client = JedisUtils.getJedisClient()

        itr.foreach(tp => {
          client.set("P-every_time"+tp._1._1.substring(0, 8), tp._1._2)
          client.set("P-every_succNum"+tp._1._1.substring(0, 8), tp._2(1).toString)
          client.expire("P-"+tp._1._1.substring(0, 8), 60 * 60 * 24 * 2)
        })
        client.close()
      })


      // 每分钟的数据分布情况统计
      baseData.map(t => ("C-"+t._5, t._3)).reduceByKey((list1, list2) => {
        (list1 zip list2) map(x => x._1 + x._2)
      }).foreachPartition(itr => {

        val client = JedisUtils.getJedisClient()

        itr.foreach(tp => {
          client.set( "succ_everymin"+tp._1, tp._2(1).toString)
          client.set("money_everymin"+tp._1,  tp._2(2).toString)
          client.expire(tp._1, 60 * 60 * 24 * 2)
        })
        client.close()
      })




      // 记录偏移量
      offsetRanges.foreach(osr => {
        DB.autoCommit{ implicit session =>
          sql"REPLACE INTO streaming_offset_1(topic, groupid, partitions, offset) VALUES(?,?,?,?)"
            .bind(osr.topic, load.getString("kafka.group.id"), osr.partition, osr.untilOffset).update().apply()
        }
        // println(s"${osr.topic} ${osr.partition} ${osr.fromOffset} ${osr.untilOffset}")
      })

    })


    // 结果存入到redis, 将偏移量存入到mysql
    // 启动程序，等待程序终止
    ssc.start()
    ssc.awaitTermination()
  }


}
