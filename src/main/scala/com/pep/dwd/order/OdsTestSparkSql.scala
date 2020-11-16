package com.pep.dwd.order

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}


object OdsTestSparkSql {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("test-demo")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    spark.sql("use dwd")
    val querySql =
      """
        |select product_id from action_do_log where put_date='20201102'
      """.stripMargin

    val rdd_1: RDD[Row] = spark.sql(querySql).rdd

    //val javaList = new util.ArrayList[String]()
    //val broadcast = sc.broadcast[util.ArrayList[String]](javaList)

    val accumulator = sc.longAccumulator("accumulator")

    val rdd_2 = rdd_1.map(x => {
      val productId = x.getAs[String]("product_id")
      if ("1213".equals(productId)) {
        accumulator.add(1)
      }
      x
    })

    println("累加器的结果为：" + accumulator.value)
    rdd_2.take(1)
    spark.stop()
    sc.stop()
  }
}
