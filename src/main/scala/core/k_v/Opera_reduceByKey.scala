package core.k_v

import org.apache.spark.sql.SparkSession

/**
  * @author xiaoran
  * @date 2020/02/26
  *
  */
object Opera_reduceByKey {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("Opera_reduceByKey").master("local").getOrCreate()

    val kvRDD = spark.sparkContext.makeRDD(Seq("lakers"->"james","lakers"->"kobe","thunder"->"westbrook","thunder"->"george","bucks"->"Antetokounmpo"))

    kvRDD.reduceByKey{(a,b)=>
      a +" & "+ b
    }.foreach(println)
  }
}
