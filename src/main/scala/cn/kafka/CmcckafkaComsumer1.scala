package cn.kafka

import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object CmcckafkaComsumer1 {


  def main(args: Array[String]): Unit = {
    //注意这里需要两个线程，一个获取数据，一个用于计算
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    //val sc = new SparkContext()
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    //val context: SQLContext = SQLContext(sc)
    //设置检查点
    //ssc.checkpoint("D:")

    //配置用于请求kafka的几个参数 把多个变量赋值
    val zkQuorum = "mini1:2181,mini2:2181,mini3:2181"
    val group = "day1_001"
    val topic = "JsonData"
    val numThread = "2"
    //把每个topic放到map里
    val topicMap: Map[String, Int] = topic.split(",").map((_, numThread.toInt)).toMap

    //调用kafka的工具类来获取kafka的数据
    val dStream: ReceiverInputDStream[(String, String)] =
      KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)

    //dSteam中，key的数据为offset，value为数据


    //实时数据分析

      /**
        * 获取到充值成功的订单笔数
        */
      val data = dStream.map(t => JSON.parseObject(t._2))
        .filter(obj=> obj.getString("serviceName")
          .equalsIgnoreCase("reChargeNotifyReq")).cache()

    val totalSucc: DStream[(String, Int)] = data.map(obj => {
      //日期
      val reqId = obj.getString("requestId")
      val day = reqId.substring(0, 8)
      //取出的标志
      val result = obj.getString("bussinessRst")
      val flag = if (result.equals("0000")) 1 else 0
      (day, flag)

    }).reduceByKey(_ + _)

       totalSucc.print
      println("充值成功的订单数:" + totalSucc.print)


      /**
        * 获取充值成功的订单金额
        */
      val totalMoney= data.map(obj=>{
           val reqId = obj.getString("requestId")
        //获取日期
        val day = reqId.substring(0, 8)
        //取出该条充值是否成功的标志
        val result = obj.getString("bussinessRst")
        val fee = if(result.equals("0000")) obj.getString("chargefee").toDouble  else 0
        (day, fee)
      }).reduceByKey(_+_)
       totalMoney.print
      println("充值成功的订单总额:" + totalMoney.print)



      /**
        * 获取充值成功的充值时长
        */
      val totalTime = data.map(obj=>{
        var reqId = obj.getString("requestId")
        //获取日期
        val day = reqId.substring(0, 8)

        //取出该条充值是否成功的标志
        val result = obj.getString("bussinessRst")
        //时 间 格 式 为: yyyyMMddHHmissSSS(( 年月日时分秒毫秒)
        val endTime = obj.getString("receiveNotifyTime")
        val startTime =  reqId.substring(0, 17)
        val format = new SimpleDateFormat("yyyyMMddHHmmssSSS")
        val costTime =  if(result.equals("0000")) format.parse(endTime).getTime - format.parse(startTime).getTime else 0

        (day, costTime)
      }).reduceByKey(_+_)

      println("充值成功的充值时长:" + totalTime.print)
    totalTime.print





    ssc.start()
    ssc.awaitTermination()
  }
}

























//          //var database = JSON.parseObject(baseData)
//          //是否成功：
//          val result = database.getString("bussinessRst")
//          val isSucc:Double= if (result.equals("0000")) 1 else 0
//          val totalSucc=database.

//          val fee:Double = if (result.equals("0000")) database.getDouble("chargefee") else 0.0
//          val receiveTime = database.getString("receiveNotifyTime")
//          var starttime: String = database.getString("requestId")
//          val pCode = database.getString("provinceCode")
//          val province = getProvince(pCode)
//          val costime = if (result.equals("0000")) caculateRqt( receiveTime ,starttime) else 0
//
//         // ("单号id：" + 1,"开始日期：" + starttime.substring(0, 10), "开始日期加小时：" + starttime.substring(0, 12), "消费是否成功:" + isSucc, "订单金额:" + fee, "消费时间:" + costime)
//         ("单号id：" + 1,"开始日期：" +  starttime.substring(0, 8), "开始日期加小时：" + starttime.substring(0, 12),
//           List[Double](1,isSucc,fee,costime.toDouble),starttime.substring(0, 10),province)
          //           pCode match {
          //
          //                  case "100"=>"北京"
          //                  case "200"=>"广东",
          //                  case "210"=>"上海",
          //                  case "220"=>"天津",
          //                  case "230"=>"重庆",
          //                  case "240"=>"辽宁",
          //                  case "250"=>"江苏",
          //                  case "270"=>"湖北",
          //                  case "280"=>"四川",
          //                  case "290"=>"陕西",
          //                  case "311"=>"河北",
          //                  case "351"=>"山西",
          //                  case "371"=>"河南",
          //                  case "431"=>"吉林",
          //                  case "451"=>"黑龙江",
          //                  case "471"=>"内蒙古",
          //                  case "531"=>"山东",
          //                  case "551"=>"安徽",
          //                  case "571"=>"浙江",
          //                  case "591"=>"福建",
          //                  case "731"=>"湖南",
          //                  case "771"=>"广西",
          //                  case "791"=>"江西",
          //                  case "851"=>"贵州",
          //                  case "871"=>"云南",
          //                  case "891"=>"西藏",
          //                  case "898"=>"海南",
          //                  case "931"=>"甘肃",
          //                  case "951"=>"宁夏",
          //                  case "971"=>"青海",
          //                  case "991"=>"新疆"
          //
          //        }

//          //("开始日期：" + starttime.substring(0, 12), "开始日期加小时："+starttime.substring(0, 10), List[Double](1,isSucc,fee,costime.toDouble),pCode, starttime.substring(0, 12))
//        })
//
//          data.foreach(println)
//      println("111111111111111")
//
//      /**
//        * 统计全网充值订单，充值金额，充值成功率，以及充值平均时长
//        *
//        */
//      var res1 = data.map(t => (t._3, t._4)).reduceByKey((list1,list2)=>{
//       (list1 zip list2).map(x => x._1 + x._2)
//        })
//      //res1.foreach(println)
//
//         res1.map(itr=>{
//           val  total=itr._2(0).toLong
//           val  succ =(itr._2(1).toLong/total)*1.0
//           val  money=itr._2(2)
//           val  timer=itr._2(3).toLong
//           ("全网充值订单:"+total,"充值金额:"+money,"充值成功率:"+succ,"充值平均时长"+total)
//         })
//
//
////              res1.foreachPartition(itr => {
////
////              val client = getJedisClient()
////
////              itr.foreach(tp => {
////                client.hincrBy(tp._1, "total", tp._2(0).toLong)
////                client.hincrBy(tp._1, "succ", tp._2(1).toLong)
////                client.hincrByFloat(tp._1, "money", tp._2(2))
////                client.hincrBy(tp._1, "timer", tp._2(3).toLong)
////
////               client.expire(tp._1, 60 * 60 * 24 * 2)
////              })
////              client.close()
////            })
//        res1.foreach(println)
//      print("2222222222222222222222222")
//
//      /**
//        * 每个小时的数据分布情况统计
//        */
//      //
//      var res2: RDD[(String, List[Double])] = data.map(t => (t._5, t._4)).reduceByKey((list1, list2) => {
//        (list1 zip list2).map(x => x._1 + x._2)
//      })
//      res2
//
////
////        .foreachPartition(itr => {
////
////        val client = getJedisClient()
////
////        itr.foreach(tp => {
////          // B-2017111816
////          client.hincrBy(tp._1, "total", tp._2(0).toLong)
////          client.hincrBy(tp._1, "succ", tp._2(1).toLong)
////
////          client.expire(tp._1, 60 * 60 * 24 * 2)
////        })
////        client.close()
////      })
//      res2.foreach(println)
//
//
//      /**
//        * 每个省份充值成功数据
//        */
//                                                                       //前10  省     1次数
//      var res4: RDD[((String, String), List[Double])] = data.map(t => ((t._5, t._6), t._4)).reduceByKey((list1, list2) => {
//        (list1 zip list2).map(x => x._1 + x._2)
//      })
//      res4.map(key=>{
//        val time=key._1._1.substring(0,8)
//        val province=key._1._2
//        val count=key._2(1).toLong
//        ("时间："+time,"省份:"+province,"成功次数:"+count)
//      })
//      res4.foreach(println)
//
////        res4.foreachPartition(itr => {
////
////        val client = getJedisClient()
////
////        itr.foreach(tp => {
////          client.hincrBy("P-"+tp._1._1.substring(0, 8), tp._1._2, tp._2(1).toLong)
////          client.expire("P-"+tp._1._1.substring(0, 8), 60 * 60 * 24 * 2)
////        })
////        client.close()
////      })
//
//      /**
//        * 每分钟的数据分布情况
//        */
//      var res5: RDD[(String, List[Double])] = data.map(t => (t._3, t._4)).reduceByKey((list1, list2) => {
//        (list1 zip list2) map (x => x._1 + x._2)
//      })
//      res5
//          .map(key=>{
//            val time=key._1
//            val  value=key._2
//            (time,value)
//          })
//            res5.foreach(println)
//      print("555555555555555")
//
//
//
//    })
//
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//
//
//  val jedisPool = new JedisPool(new GenericObjectPoolConfig(), "mini1")
//
//  def getJedisClient(): Jedis = {
//    jedisPool.getResource
//  }
//  //获得相应的省份
//  def getProvince(key: String): String = {
//    key match {
//      case "100" => "北京"
//      case "200" => "广东"
//      case "210" => "上海"
//      case "220" => "天津"
//      case "230" => "重庆"
//      case "240" => "辽宁"
//      case "250" => "江苏"
//      case "270" => "湖北"
//      case "280" => "四川"
//      case "290" => "陕西"
//      case "311" => "河北"
//      case "351" => "山西"
//      case "371" => "河南"
//      case "431" => "吉林"
//      case "451" => "黑龙江"
//      case "471" => "内蒙古"
//      case "531" => "山东"
//      case "551" => "安徽"
//      case "571" => "浙江"
//      case "591" => "福建"
//      case "731" => "湖南"
//      case "771" => "广西"
//      case "791" => "江西"
//      case "851" => "贵州"
//      case "871" => "云南"
//      case "891" => "西藏"
//      case "898" => "海南"
//      case "931" => "甘肃"
//      case "951" => "宁夏"
//      case "971" => "青海"
//      case "991" => "新疆"
//
//    }
//    key
//  }
//
//  //工具获得消耗时间差
//  def caculateRqt(startTime: String, endTime: String): Long = {
//
//    val dateFormat = new java.text.SimpleDateFormat("yyyyMMddHHmmssSSS")
//
//    val st = dateFormat.parse(startTime.substring(0, 17)).getTime
//    val et = dateFormat.parse(endTime).getTime
//    et - st
//  }
//}
