package sparksql

import org.apache.spark.sql.SparkSession

/**
  * @author xiaoran
  * @date 2020/03/01
  *
  * 主要介绍  RDD  DateFrame  DataSet的区别
  */
object SparkSqlThree_Transform {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkSqlOne_Read").master("local").getOrCreate()

    //读取数据，创建dataframe
    val frame = spark.read.json("src/main/scala/sparksql/user.json")

    import spark.implicits._
    //创建rdd
    val rdd = spark.sparkContext.makeRDD(List((1,"关羽","青龙偃月刀"),
      (2,"张飞","丈八点蛇矛"),(3,"赵云","龙胆亮银枪"),(4,"马超","虎头湛金枪"),(5,"黄忠","九凤朝阳刀")))

    // rdd转换为 DF
    //当rdd没有结构的时候，需要为df添加结构
    val df = rdd.toDF("id","name","enginery")

    df.show()

    // df转化为ds
    //需要为ds添加类型约束
    val ds = df.as[User]
    df.show()

    //rdd转化为ds
    val ds1 = rdd.toDS()
    ds1.show()

    // 转化为rdd
    //有结构，无类型
    val rdd1=df.rdd

    //有结构，有类型
    val rdd2=ds.rdd

    println("xxxxxxxxxxx")
    rdd1.foreach{row=>
      println(row.getString(1))
    }

    rdd2.foreach{
      row=>
        println(row.enginery)
    }

  }
}

case class User(id:Int,name:String,enginery:String)