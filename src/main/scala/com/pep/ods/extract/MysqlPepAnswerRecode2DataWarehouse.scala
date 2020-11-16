package com.pep.ods.extract

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  *   hadoop fs -rmr /pep_cloud/business/ods/a_answer_recode/count_date=20200000
  */
object MysqlPepAnswerRecode2DataWarehouse {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RUN-MysqlPepAnswerRecode2DataWarehouse")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val props = new java.util.Properties
    val a_answer_recode = "a_answer_recode_7"
    props.setProperty("user","diandu_user")
    props.setProperty("password","P0#u2vM8Sw0Q2pA3")
    props.setProperty("url","jdbc:mysql://rm-2zeejnctt79xv52ot.mysql.rds.aliyuncs.com:3306/jx_click")
    val default_row_timestamp = System.currentTimeMillis()
    val default_row_status = "1"

    val a_answer_recode_sql =
      s"""
         |create table if not exists ods.a_answer_recode(
         |id string comment '',
         |user_id string comment '',
         |answer_type string comment '作答类型',
         |ctree_id string comment '教材id',
         |rel_id string comment '关联id',
         |rel_id_ext string comment '扩展关联id',
         |group_id string comment '',
         |group_name string comment '',
         |chapter_name string comment '扩展内容',
         |total_points string comment '总分',
         |score string  comment '得分',
         |score_ext string comment '扩展得分',
         |recode_details string comment '记录详情',
         |start_time string comment '开始时间',
         |end_time string comment '结束时间',
         |time_consume string comment '耗时',
         |complete_status string comment '完成状态',
         |content string comment ''
         |) partitioned by (count_date string)
         |row format serde 'org.apache.hive.hcatalog.data.JsonSerDe'
         |location '/pep_cloud/business/ods/a_answer_recode'
      """.stripMargin
    spark.sql(a_answer_recode_sql)
    /**
      * 导入表 a_answer_recode
      */
    //定义SparkSQL读取Mysql的线程数量，以及线程的读取数据量
    val predicates1 = Array(
      0,
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10,
      11,
      12,
      13,
      14,
      15,
      16,
      17,
      18,
      19
    ).map( item => s"mod(id,20)=${item}")
    val zykResourceDF: DataFrame = spark.read.format("jdbc").jdbc(props.getProperty("url"),a_answer_recode,predicates1,props)
    //创建临时表
    zykResourceDF.createOrReplaceTempView("a_answer_recode_tmp")
    //将临时表写入数仓
    val etlSql1 =
      s"""
         |select * from a_answer_recode_tmp
       """.stripMargin
    val etlDF = spark.sql(etlSql1)
    val write_path1 = "hdfs://emr-cluster/pep_cloud/business/ods/a_answer_recode/count_date=20200000"
    etlDF.write.mode(SaveMode.Append).json(write_path1)

    spark.stop()
  }
}
