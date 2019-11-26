package core

import org.apache.spark.sql.SparkSession

/**
  * @author xiaoran
  * @date 2019/11/26
  *
  * glom 转换算子:  将RDD[]中每一个分区的数据，变成RDD[Array] 数据
  */
object Opera_glom {

  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local").appName("Opera_glom").getOrCreate()

    val listRDD = spark.sparkContext.makeRDD(1 to 18,4)

    val glomRDD = listRDD.glom()

    glomRDD.foreach(part=>
    {
      println(part.mkString(","))
    })
  }

}
