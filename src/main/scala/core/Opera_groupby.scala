package core

import org.apache.spark.sql.SparkSession

/**
  * @author xiaoran
  * @date 2020/02/24
  *
  *  groupby算子，按照规则进行分组
  *
  */
object Opera_groupby {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("Opera_groupby").getOrCreate()
    val arr=Array(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19)

    val rdd = spark.sparkContext.parallelize(arr)

    val newRDD = rdd.groupBy(i=>i%3)
    newRDD.collect().foreach(println)

  }
}
