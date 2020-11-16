package com.pep.ods.history

import com.pep.common.Constants
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * original_action_log 2 action_log
  */

object OdsOriginalLog2ActionLog {

  def main(args: Array[String]): Unit = {
    if (args.length > 0) {
      println("args length is "+args.length)
      doOdsOriginalLogTransformJob(args)
    }
  }

  def doOdsOriginalLogTransformJob(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("JOB-OdsOriginalLog2ActionLog")
    conf.set("spark.sql.shuffle.partitions", Constants.dws_shuffle_partitions)

    val a_answer_recode_sql =
      s"""
        |create external table  a_answer_recode(
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

    val spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
    import spark.sql
    sql("show databases").show()
    sql("use ods")
    sql("msck repair table original_action_log")
    for (i <- 0 to args.length - 1) {
      val putDate = args(i)

      val sql_1 =
        s"""
           |insert overwrite action_log PARTITION (put_date)
           |select '',
           |       remote_addr,
           |       request_time,
           |       log_version,
           |       start_time,
           |       end_time,
           |       region,
           |       product_id,
           |       hardware,
           |       os,
           |       soft,
           |       active_user,
           |       active_org,
           |       active_type,
           |       passive_obj,
           |       passive_type,
           |       from_prod,
           |       from_pos,
           |       company,
           |       action_title,
           |       action_type,
           |       request,
           |       request_param,
           |       group_type,
           |       group_id,
           |       result_flag,
           |       result,
           |       put_date
           |from original_action_log
           |where put_date = '$putDate'
           |  and start_time is not null
           |  and not (product_id = '1213' and action_title = 'sys_100001') distribute by substring(start_time, 8, 10)
       """.stripMargin

      sql(sql_1)
    }

    spark.stop()
  }

}



