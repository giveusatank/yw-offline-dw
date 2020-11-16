package com.pep.ods.extract

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *   将教材加工库上Mysql中jxw_platfrom库中的p_textbook全量业务用户表导入数仓的ods
  *   层的ods_jxw_platform_p_textbook的put_date=20190000分区中
  *
  *   hadoop fs -rmr /pep_cloud/business/ods/ods_jxw_platform_p_resource/put_date=20190000
  */
object MysqlJxwOrganizationData2DW {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RUN-MysqlJxwOrganizationData2DW")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val props = new java.util.Properties
    val tableName = "gxxm_a_organization"
    props.setProperty("user","pdadmin")
    props.setProperty("password","R8$7Zo319%0tUi")
    props.setProperty("url","jdbc:mysql://rm-2zefoq89s74i5bfal.mysql.rds.aliyuncs.com:3306/yw_bus")

    //定义SparkSQL读取Mysql的线程数量，以及线程的读取数据量
    val predicates = Array(
      0,
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9
    ).map(x => s"mod(id,10)=${x}")

    val mysqlReadDF: DataFrame = spark.read.format("jdbc").jdbc(props.getProperty("url"),tableName,predicates,props)

    val createSql =
      s"""
         |create table if not exists ods.ods_organization(
         |product_id     string,
         |company        string,
         |id             string,
         |reg_name       string,
         |region_code    string,
         |name           string,
         |row_timestamp  string,
         |row_status     string
         |) partitioned by (put_date string)
         |row format serde 'org.apache.hive.hcatalog.data.JsonSerDe'
         |location '/pep_cloud/business/ods/ods_organization'
      """.stripMargin
    spark.sql(createSql)

    //创建临时表

    val default_row_timestamp = System.currentTimeMillis()
    val default_row_status = "1"



    mysqlReadDF.createOrReplaceTempView("a_organization")
    //将临时表写入数仓
    val etlSql =
      s"""
         |select '111307' as product_id,'450000001' as company,id,reg_name,s_administrative_code as region_code,name,'$default_row_timestamp' as row_timestamp,'$default_row_status' as row_status from a_organization
       """.stripMargin

    val etlDF: DataFrame = spark.sql(etlSql)
    //20200000 智慧教学平台
    //20200001 广西项目
    val write_path = "hdfs://emr-cluster/pep_cloud/business/ods/ods_organization/put_date=20200001"
    etlDF.write.mode("overwrite").json(write_path)
    spark.stop()
  }
}
