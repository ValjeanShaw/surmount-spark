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


    /**
      * 全局表和临时表的区别
      * 局部表只存在于session中，不能跨session访问，全局临时表不存在session中
      *
      * 全局表查询要加 global_temp.
      */
    frame.createOrReplaceGlobalTempView("userTable")
    spark.sql("select * from global_temp.userTable").show()


  }
}
