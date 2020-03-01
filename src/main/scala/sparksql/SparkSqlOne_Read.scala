package sparksql

import org.apache.spark.sql.SparkSession

/**
  * @author xiaoran
  * @date 2020/03/01
  *
  */
object SparkSqlOne_Read {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkSqlOne_Read").master("local").getOrCreate()

    //读取数据，创建dataframe
    val frame = spark.read.json("src/main/scala/sparksql/user.json")
    frame.show()

  }
}
