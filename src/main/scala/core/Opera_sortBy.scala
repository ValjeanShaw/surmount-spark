package core

import org.apache.spark.sql.SparkSession

/**
  * @author xiaoran
  * @date 2020/02/25
  *
  *      sortBy算子，排序算子
  *
  */
object Opera_sortBy {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Opera_sortBy").master("local").getOrCreate()
    val rdd = spark.sparkContext.makeRDD(Seq(4,3345,45,65,2,46,8,43,6,98,9,55))

    //function是操作   默认升序，false降序

    //如果函数的参数在函数体内只出现一次，则可以使用下划线代替
    val newRDD1=rdd.sortBy(_/2)
    //函数 用具体参数
    val newRDD2=rdd.sortBy(x=>x%2)
    newRDD1.foreach(println)
    newRDD2.foreach(println)
  }
}
