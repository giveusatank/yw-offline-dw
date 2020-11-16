package com.pep.dim

import java.text.SimpleDateFormat
import java.util
import java.util.{ArrayList, Calendar, Date, List}

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object DimLast30Day2DW {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RUN-DimLast30Day2Dw")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    spark.sql("drop table if exists dim.dim_last30day")
    val createSql =
      s"""
         |create table if not exists dim.dim_last30day(
         |ymd              string,
         |count_date       string
         |)
         |STORED AS parquet
      """.stripMargin
    spark.sql(createSql)

    val schema = StructType(
      Seq(
        StructField("ymd", StringType, true),
        StructField("count_date", StringType, true)
      )
    )
    val format = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance()
    cal.add(Calendar.DAY_OF_YEAR, -1)
    val list = last30Day()
    val df = spark.createDataFrame(list, schema)
    df.write.mode("overwrite").parquet("hdfs://emr-cluster/user/hive/warehouse/dim.db/dim_last30day")
    spark.stop()
  }

  def last30Day(): util.ArrayList[Row] = {
    val list = new util.ArrayList[Row]()
    try {
      val format = new SimpleDateFormat("yyyyMMdd")
      val cal = Calendar.getInstance()
      cal.add(Calendar.DAY_OF_YEAR, -1)
      val startDate = format.format(cal.getTime)
      val calYmd = Calendar.getInstance()
      for (a <- 1 to 30) {
        calYmd.add(Calendar.DAY_OF_YEAR, -1)
        val ymd = format.format(calYmd.getTime)
        val row = Row(ymd, startDate)
        list.add(row)
      }

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    list
  }

}

