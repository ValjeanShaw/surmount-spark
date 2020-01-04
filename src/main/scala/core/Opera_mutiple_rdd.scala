package core

import org.apache.spark.sql.SparkSession

/**
  * @author xiaoran
  * @date 2020/01/05
  *
  *      多个rdd之间的操作
  *
  */
object Opera_mutiple_rdd {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("Opera_mutiple_rdd").master("local").getOrCreate()
    val rdd1 = spark.sparkContext.makeRDD(1 to 18,5)
    val rdd2 = spark.sparkContext.makeRDD(81 to 98,5)
    /**
      * union
      */
    val rdd3=rdd1.union(rdd2)
    rdd3.foreach(println)

    /**
      * 并集
      *
      */
    val rdd4=rdd1.subtract(rdd2)


    /**
      * 交集
      */
    val rdd5=rdd1.intersection(rdd2)


    /**
      * 拉链操作  zip
      * 要求很严格：两个RDD之间数量和分区数都要相等
      */
    val rdd6=rdd1.zip(rdd2)
  }
}
