package scylladbwrite

import com.datastax.driver.core.{BoundStatement, ConsistencyLevel}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @author xiaoran
  * @date 2019/11/22
  *
  */
object WriteData extends Logging{
    def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder().master("local").appName("WriteData").getOrCreate()
      val tableName="hbase.write_test"
      val numRDD = spark.sparkContext.makeRDD(1 to 100000 map { row =>
        (("%06d".format(row)), "test")
      })

      val sql = s"insert into $tableName(rowkey,a) values(?,?);"

      insertScylladbData(numRDD, sql)
    }

    def insertScylladbData(dataRdd: RDD[(String, String)], sql: String): Unit = {
      val startTime=System.currentTimeMillis()

      dataRdd.foreachPartition(itera =>{
        val scylladbClient = new ScylladbClient("172.23.7.13,172.23.7.14,172.23.7.16",9042,ConsistencyLevel.ANY);
        scylladbClient.initSession()
        val prepare = scylladbClient.prepare(sql)

        while(itera.hasNext){
          val data = itera.next()
          val boundStatement=new BoundStatement(prepare)
          scylladbClient.insertAsync(boundStatement.bind(data._1,data._2))
        }
        scylladbClient.destory()
      })
      println("耗时(ms)："+(System.currentTimeMillis()-startTime))
    }


}
