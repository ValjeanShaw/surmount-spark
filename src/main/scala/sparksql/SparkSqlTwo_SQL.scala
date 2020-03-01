package sparksql

import org.apache.spark.sql.SparkSession

/**
  * @author xiaoran
  * @date 2020/03/01
  *
  */
object SparkSqlTwo_SQL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkSqlOne_Read").master("local").getOrCreate()

    //读取数据，创建dataframe
    val frame = spark.read.json("src/main/scala/sparksql/user.json")

    //采用sql的方式创建一张表
    frame.createOrReplaceTempView("user")

    spark.sql("select * from user").show()


  }
}
