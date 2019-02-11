package cn.sheep

import org.apache.spark.{SparkConf, SparkContext}

object test1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test1").setMaster("local")
    val sc = new SparkContext(conf)

    val list1: List[Int] =List(1,2,3,4,12)
    val list2: List[Int] =List(5,6,7,8,9)
    val list3: List[Int] =List(2,3,4,5,6)
    val list4: List[Int] =List(7,8,9,0,3)
    val list5: List[Int] =List(7)

   val res = list5.zip(list1)
    list1.zip(list2).foreach(println)

    res.foreach(println)


          var n =100
          for(i<-1 to n)
          println("执行次数:"+i)



  }

}
