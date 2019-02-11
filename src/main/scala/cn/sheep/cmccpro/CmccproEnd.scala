package cn.sheep.cmccpro

import cn.sheep.utils.{JedisUtils, Utils}
import com.alibaba.fastjson.JSON
import com.typesafe.config.ConfigFactory
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc._
import scalikejdbc.config.DBs

object CmccproEnd {

  // 屏蔽日志
  Logger.getLogger("org.apache").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    // StreamingContext
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[2]")
    sparkConf.setAppName("实时统计")

    //默认采用org.apache.spark.serializer.JavaSerializer
    //这是最基本的优化
    //将rdd以序列化格式来保存以减少内存的占用
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // rdd压缩
    sparkConf.set("spark.rdd.compress", "true")
    //batchSize = partitionNum * 分区数量 * 采样时间
    //吞吐量    =分区数   *   每秒采样数量
    //spark.streaming.kafka.maxRatePerPartition   默认没有设置，也就是做没做限制。
    //如果做限制100，那么每秒最大吞吐就是100条。
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "100")
    // 创建 StreamingContext                    时间片为2秒
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("error")
    val ssc = new StreamingContext(sparkConf, Seconds(2))


    //加载配置文件
    val load = ConfigFactory.load()

    // 创建kafka相关参数
    val kafkaParams = Map(
      "metadata.broker.list" -> load.getString("kafka.broker.list"),
      "group.id" -> load.getString("kafka.group.id"),
      "auto.offset.reset" -> "smallest"

    )
    //得到配置文件（application.conf）里的参数
    val topics = load.getString("kafka.topics").split(",").toSet

    // 从kafka弄数据 --- 从数据库中获取到当前的消费到的偏移量位置 -- 从该位置接着往后消费
    // 加载配置信息
    //scalikeJDBC  连接  mysql  校验  offset 值
    //目的：保证了每次消费都是最新的offset值
    DBs.setup()
    //将 Topic  partitions 放入TopicAndPartition  类中，类中有
    //两个方法，一是转化为元组，二是转成字符串
    val fromOffsets: Map[TopicAndPartition, Long] = DB.readOnly { implicit session =>
      sql"select * from streaming_offset_1 where groupid=?".bind(load.getString("kafka.group.id"))
            .map(rs => {
        (TopicAndPartition(rs.string("topic"), rs.int("partitions")),
          rs.long("offset"))
      }).list().apply()
    }.toMap    //最后要toMap一下，因为前面的返回值已经给定


    val stream = if (fromOffsets.size == 0) { // 假设程序第一次启动
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    } else {
      //如果程序不是第一次启动
      //首先获取Topic和partition、offset
      var checkedOffset = Map[TopicAndPartition, Long]()
      //获得KafkaCluster 实例化对象，
      //目的：得到getEarliestLeaderOffsets方法，得到这次最早offset值
      // 加载kafka的配置
      val kafkaCluster = new KafkaCluster(kafkaParams)
      //首先获取Kafka中的所有Topic partition offset
      val earliestLeaderOffsets = kafkaCluster.getEarliestLeaderOffsets(fromOffsets.keySet)
      //然后开始进行比较大小，用MySQL中的offset和kafka的offset进行比较
      if (earliestLeaderOffsets.isRight) {
        //取到我们需要的Map
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
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc,
                                                              kafkaParams, checkedOffset, messageHandler)
    }

    /**
      * receiver 接受数据是在Executor端 cache -- 如果使用的窗口函数的话，没必要进行cache, 默认就是cache， WAL ；
      * 如果采用的不是窗口函数操作的话，你可以cache, 数据会放做一个副本放到另外一台节点上做容错
      * direct 接受数据是在Driveeer端
      */
    // 处理数据 ---- 根据业务--需求
    stream.foreachRDD(rdd => {
      // rdd.foreach(println)
      //得到offset，将之转换为 HasOffsetRanges 类型
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        //开始处理stream 中的数据，先得到value,使用JOSN,再将之转换为JSON格式，方便取值
      val baseData = rdd.map(t => JSON.parseObject(t._2))
        //过滤
        .filter(_.getString("serviceName").equalsIgnoreCase("reChargeNotifyReq"))
        .map(obj => {

          // 判断该条日志是否是充值成功的日志
          val result = obj.getString("bussinessRst")
          //充值的金额
          val fee = obj.getDouble("chargefee")
          //获取数据对应的省份的code
          val provinceCode = obj.getString("provinceCode")
          // 充值发起时间和结束时间
          val requestId = obj.getString("requestId")
          // 数据当前日期，小时，分钟
          val day = requestId.substring(0, 8)
          val hour = requestId.substring(8, 10)
          val minute = requestId.substring(10, 12)
          val receiveTime = obj.getString("receiveNotifyTime")
          val  flag= if(!result.equals("0000")) 1  else 0



          //充值花费的时间
          val costTime = Utils.caculateRqt(requestId, receiveTime)
          val succAndFeeAndTime: (Double, Double, Double) = if (result.equals("0000")) (1, fee, costTime) else (0, 0, 0)
          (day, hour, minute, List[Double](1, succAndFeeAndTime._1, succAndFeeAndTime._2, succAndFeeAndTime._3,flag),
            provinceCode)
        }).cache()            //做缓存，下面多次用到


      // 实时报表 -- 业务概况

      /**
        * 1)统计全网的充值订单量, 充值金额, 充值成功率及充值平均时长.
        */

      /**
        * 获取实时的成功订单数和成交金额
        *
        * @param baseData
        */
      var res: RDD[((String, String, String), List[Double])] =
//        (day, hour, minute, List[Double](1, succAndFeeAndTime._1, succAndFeeAndTime._2, succAndFeeAndTime._3),
//          provinceCode)
        baseData.map(tp => ((tp._1, tp._2, tp._3), List(tp._4(1), tp._4(2))))
          //reduceByKey后取出List里面的元素，list1代表第一个 List,list2代表后面一个List
          .reduceByKey((list1, list2) => {
          //相同位置上元素都是1，形式：（1,金额）,
          //                            (1,金额)
          //  进行累加，得到每分钟的订单数量
            list1.zip(list2).map(tp => tp._1 + tp._2)
          })
             //存入redis :
        res.foreachPartition(partition => {
          //先获得方法，到获得连接池，到获得连接池对象，到最后连接获得redis连接池资源resource,
          val jedis = JedisUtils.getJedisClient()
          partition.foreach(tp => {

            // redis  hash类型  自增存入数据
            //订单数                     每天                           每小时  的 每分钟    订单总数量
            jedis.hincrBy("D-" + tp._1._1, "Province" + tp._1._2 + tp._1._3, tp._2(0).toLong)
            //每分钟的成交金额                                                                订单总金额
            jedis.hincrBy("D-" + tp._1._1, "salary" + tp._1._2 + tp._1._3, tp._2(1).toLong)
            // key的有效期                                    1天
            jedis.expire("D-" + tp._1._1, 24 * 60 * 60)
          })
          //连接池关闭
          jedis.close()
        })

      /**
        * 查看每天各个省份的失败次数
        * pcode2pname.value.getOrElse(tp._1._2, tp._1._2).toString
        *
        * @param baseData
        * @param pcode2pname
        */
      //        (day, hour, minute, List[Double](1, succAndFeeAndTime._1, succAndFeeAndTime._2, succAndFeeAndTime._3,flag),
      //          provinceCode)
      var res1: RDD[((String, String), List[Double])] = baseData.map(tp =>
                    ((tp._1, tp._5), List(tp._4(0), tp._4(4)))).reduceByKey(
        (list1, list2) => {
          list1.zip(list2).map(t => t._1 + t._2)
        })
      res1
        .foreachPartition(partition => {
          val jedis = JedisUtils.getJedisClient()
          partition.foreach(tp => {
            val provinceName =getProvince(tp._1._2.toString)
            //val  res=(tp._2(1).toLong/tp._2(0).toLong)*100
            //                                                    次数
            jedis.hincrBy("C-" + tp._1._1, provinceName,   tp._2(1).toLong  )
            // key的有效期                                       24小时
            jedis.expire("C-" + tp._1._1, 24 * 60 * 60)


          })
          jedis.close()
        })


      /**
        * 查看每天每小时的成交量和总的订单数，充值金额
        *
        * @param baseData
        */

      //实时充值业务
      //        (day, hour, minute, List[Double](1, succAndFeeAndTime._1, succAndFeeAndTime._2, succAndFeeAndTime._3),
      //          provinceCode)
      baseData.map(tp => ((tp._1, tp._2), List(tp._4(0), tp._4(1),tp._4(2)))).reduceByKey((list1, list2) => {
        list1.zip(list2).map(tp => tp._1 + tp._2)
      })
        .foreachPartition(partition => {
          val jedis = JedisUtils.getJedisClient()
          partition.foreach(tp => {
            //                            天                           小时      次数
            jedis.hincrBy("B-" + tp._1._1, "total" + tp._1._2, tp._2(0).toLong)
            //                                                                    单量
            jedis.hincrBy("B-" + tp._1._1, "succ" + tp._1._2, tp._2(1).toLong)
            //                                                                    金额
            jedis.hincrBy("B-" + tp._1._1, "succ" + tp._1._2, tp._2(2).toLong)
            // key的有效期
            jedis.expire("B-" + tp._1._1, 24 * 60 * 60)
          })
          jedis.close()
        })


      /**
        * 查看总的金额 成交量
        *
        * @param baseData
        */
      //实时充值业务
      //        (day, hour, minute, List[Double](1, succAndFeeAndTime._1, succAndFeeAndTime._2, succAndFeeAndTime._3),
      //          provinceCode)
      baseData.map(tp => (tp._1, tp._4)).reduceByKey((list1, list2) => {
        list1.zip(list2).map(tp => tp._1 + tp._2)
      })
        .foreachPartition(partition => {
          val jedis = JedisUtils.getJedisClient()
          partition.foreach(tp => {
            jedis.hincrBy("A-" + tp._1, "total", tp._2(0).toLong)
            jedis.hincrBy("A-" + tp._1, "succ", tp._2(1).toLong)
            jedis.hincrByFloat("A-" + tp._1, "money", tp._2(2))
            jedis.hincrBy("A-" + tp._1, "cost", tp._2(3).toLong)
            //  jedis.hget()
            // key的有效期
            jedis.expire("A-" + tp._1, 24 * 60 * 60)
          })
          jedis.close()
        })





      // 记录偏移量
      offsetRanges.foreach(osr => {
        DB.autoCommit { implicit session =>
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

  def getProvince(key: String): String = {
    key match {
      case "100" => "北京"
      case "200" => "广东"
      case "210" => "上海"
      case "220" => "天津"
      case "230" => "重庆"
      case "240" => "辽宁"
      case "250" => "江苏"
      case "270" => "湖北"
      case "280" => "四川"
      case "290" => "陕西"
      case "311" => "河北"
      case "351" => "山西"
      case "371" => "河南"
      case "431" => "吉林"
      case "451" => "黑龙江"
      case "471" => "内蒙古"
      case "531" => "山东"
      case "551" => "安徽"
      case "571" => "浙江"
      case "591" => "福建"
      case "731" => "湖南"
      case "771" => "广西"
      case "791" => "江西"
      case "851" => "贵州"
      case "871" => "云南"
      case "891" => "西藏"
      case "898" => "海南"
      case "931" => "甘肃"
      case "951" => "宁夏"
      case "971" => "青海"
      case "991" => "新疆"

    }
    key
  }
}
