package core.k_v

import org.apache.spark.sql.SparkSession

/**
  * @author xiaoran
  * @date 2020/02/26
  *
  */
object Opera_aggregateByKey {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("Opera_aggregateByKey").getOrCreate()

    val kvRDD = spark.sparkContext.makeRDD(
      Seq("lakers"->12,"thunder"->56,"laker"->100,"thunder"->123,"lakers"->34,"thunder"->78,"bucks"->90),2)

    kvRDD.foreach(println)

    /**
      * 需求：
      * 找出每个分区中每个key的最大值进行加和
      *
      * zeroValue是初始值，用来和每个rdd中的值进行判断
      * 第一个函数：分区内操作
      * 第二个函数：分区间操作
      *
      */

    val newRDD = kvRDD.aggregateByKey(10)(Math.max(_,_),_+_)

    newRDD.foreach(println)

  }
}
