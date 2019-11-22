package core

import org.apache.spark.sql.SparkSession

/**
  * @author xiaoran
  * @date 2019/11/21
  *
  */
object CreateRDD {
  def main(args: Array[String]): Unit = {


    /**
      * 内存方式创建RDD
      * makeRDD
      * parallelize
      */
    val spark=SparkSession.builder().master("local").appName("CreateRDD").getOrCreate()

    val rdd1=spark.sparkContext.makeRDD(List(1,2,3,4,5))

    val rdd2=spark.sparkContext.parallelize(Array(1,2,3,4,5))


    /**
      * 外部方式创建RDD
      * text，parquet等
      * 默认从文件读取，都是String类型
      */
    val path3="/Users/xiaoran/bigdata/surmount_spark/out/input/word.txt"
    val rdd3=spark.sparkContext.textFile(path3,2)
    rdd3.saveAsTextFile("/Users/xiaoran/bigdata/surmount_spark/out/output/res")


    val path4="parquet 格式文件路径"
    val rdd4=spark.read.parquet(path4)

  }

}
