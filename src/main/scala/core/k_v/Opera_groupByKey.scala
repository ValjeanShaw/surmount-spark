package core.k_v

import org.apache.spark.sql.SparkSession

/**
  * @author xiaoran
  * @date 2020/02/26
  *
  */
object Opera_groupByKey {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Opera_groupBy").master("local").getOrCreate()

    val kvRDD = spark.sparkContext.parallelize(Seq("lakers"->"james","lakers"->"kobe","thunder"->"westbrook","thunder"->"george","bucks"->"Antetokounmpo"))

    val newRDD = kvRDD.groupByKey()
    newRDD.foreach(println)
  }
}
