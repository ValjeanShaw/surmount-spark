package core

import org.apache.spark.sql.SparkSession

/**
  * @author xiaoran
  * @date 2019/11/26
  *
  */
object AboutMap {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local").appName("AboutMap").getOrCreate()

    val originRDD = spark.sparkContext.makeRDD(1 to 30,5)


    val rdd1=originRDD.map{
      row=>
        row*2
    }.foreach(println)



    val rdd2=originRDD.mapPartitions(iterator=>
    {
      iterator.map(row=>row*2)
    },false).foreach(println)




    val rdd3=originRDD.mapPartitionsWithIndex{
      (index,iterator)=>{
        iterator.map(row=>(row,"所在分区:"+index))
      }
    }.foreach(println)
  }
}
