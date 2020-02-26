package core.k_v

import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.sql.SparkSession

/**
  * @author xiaoran
  * @date 2020/02/25
  *
  *       partitionby算子
  */
object Opera_partitionBy {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("Opera_partitionBy").getOrCreate()

    val rdd = spark.sparkContext.makeRDD(Seq((1, 2), 2 -> 3, 3 -> 4, 4 -> 5))

    //k-v类型使用默认分区器进行分区
    val newRDD = rdd.partitionBy(new HashPartitioner(3))
    newRDD.glom().collect()


    val myRDD = rdd.partitionBy(new MyPartitions(3))

    myRDD.foreach(println)
  }

}

//自定义分区器
class MyPartitions(numPartition: Int) extends Partitioner {
  override def numPartitions: Int = numPartition

  override def getPartition(key: Any): Int = {
    key.hashCode() % numPartition
  }
}
