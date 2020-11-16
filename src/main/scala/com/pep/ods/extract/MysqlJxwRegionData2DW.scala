package com.pep.ods.extract

import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object MysqlJxwRegionData2DW {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("run-MysqlJxwRegionData2DW")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val props = new java.util.Properties()

    props.setProperty("url","jdbc:mysql://rm-2zefoq89s74i5bfal.mysql.rds.aliyuncs.com:3306/yw_bus")
    props.setProperty("user","pdadmin")
    props.setProperty("password","R8$7Zo319%0tUi")
    props.setProperty("tableName","gxxm_a_region_data")

    val regionDataDF = spark.read.jdbc(props.getProperty("url"),
      props.getProperty("tableName"),props)

    regionDataDF.createOrReplaceTempView("a_region_data_temp")

    val row_ts = new Date().getTime
    val row_status = "1"

    val querySql =
      s"""
        |select
        |'111307' as product_id,
        |'450000001' as company,
        |region_code,
        |region_name,
        |level,
        |parent_code,
        |parent_codes,
        |link_name,
        |is_leaf,
        |${row_ts} as row_timestamp,
        |${row_status} as row_status
        |from a_region_data_temp
      """.stripMargin

    //20190000 智慧教学平台
    //20190001 广西项目

    val writePath = "hdfs://emr-cluster/pep_cloud/business/ods/ods_region_data/count_date=20190001"
    spark.sql(querySql).write.mode("overwrite").json(writePath)

    spark.stop()
  }
}
