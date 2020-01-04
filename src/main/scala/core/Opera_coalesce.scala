package core

import org.apache.spark.sql.SparkSession

/**
  * @author xiaoran
  * @date 2020/01/05
  *
  * 缩减分区数
  */
object Opera_coalesce {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("Opera_coalesce").master("local").getOrCreate()
    val rdd = spark.sparkContext.makeRDD(1 to 18,5)
    rdd.coalesce(1)
  }
}
