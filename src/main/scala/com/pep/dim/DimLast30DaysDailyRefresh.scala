package com.pep.dim

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 最近三十日维度表每日刷新任务
  */
object DimLast30DaysDailyRefresh {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DimLast30DaysDailyRefresh")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    spark.sql("use dim")
    val createSql =
      """
        |create table if not exists dim.dim_last30day(
        |ymd string,
        |count_date string
        |) stored as parquet
      """.stripMargin
    val refreshSql = new StringBuilder
    refreshSql.append("insert overwrite dim.dim_last30day\n")
    for(i <- 1 to 30) {
      refreshSql.append(s"select date_sub(current_date(),${i}),date_sub(current_date(),1) ")
      refreshSql.append(if(i<30){"union all\n"} else "")
    }
    spark.sql(createSql)
    spark.sql(refreshSql.toString)
    spark.stop()
  }
}
