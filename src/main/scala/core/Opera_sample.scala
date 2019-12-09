package core

import org.apache.spark.sql.SparkSession

/**
  * @author xiaoran
  * @date 2019/11/26
  *
  */
object Opera_sample {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local").appName("sample").getOrCreate()

    val listRDD = spark.sparkContext.makeRDD(1 to 18)

    //看源码
    val sampleRDD1=listRDD.sample(false,0.4,2)
    val sampleRDD2=listRDD.sample(true,2,2)

    sampleRDD1.foreach(println)
    println("------------")
    sampleRDD2.foreach(println)

  }
}
