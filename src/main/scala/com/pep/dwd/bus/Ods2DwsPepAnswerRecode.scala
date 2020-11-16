package com.pep.dwd.bus

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.control.Breaks

object Ods2DwsPepAnswerRecode {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RUN-Ods2DwsPepAnswerRecode")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val loop = new Breaks
    val withParams = args.length > 0
    val regPattern = "^[0-9]{8}$".r
    val format = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance()
    cal.setTime(new Date())
    cal.add(Calendar.DATE, -1)
    var yestodayStr = format.format(cal.getTime)
    loop.breakable {
      for (i <- 0 until (if (withParams) args.length else 1)) {
        println(args.length)
        if (withParams) {
          if (regPattern.findPrefixOf(args(i)) == None) loop.break()
          yestodayStr = args(i)
          print(yestodayStr)
        }
        doAction(spark, yestodayStr)
      }
    }
    spark.stop()

  }

  def doAction(spark: SparkSession, yestodayStr: String) = {
    writeDwsUvTotal2DwdUserArea(spark,yestodayStr)
  }

  def writeDwsUvTotal2DwdUserArea(spark: SparkSession, yestodayStr: String) = {

    spark.sql("use dwd")

    val createSql =
      """
        |create table if not exists dwd.dwd_a_answer_recode(
        |id bigint comment '',
        |user_id string comment '',
        |answer_type string comment '作答类型',
        |ctree_id string comment '教材id',
        |rel_id string comment '关联id',
        |rel_id_ext string comment '扩展关联id',
        |group_id string comment '',
        |group_name string comment '',
        |chapter_name string comment '扩展内容',
        |total_points double comment '总分',
        |score double  comment '得分',
        |score_ext double comment '扩展得分',
        |recode_details string comment '记录详情',
        |start_time string comment '开始时间',
        |end_time string comment '结束时间',
        |time_consume double comment '耗时',
        |complete_status int comment '完成状态',
        |content string comment ''
        |)
        |partitioned by (count_date string)
        |STORED AS parquet
      """.stripMargin

    spark.sql(createSql)

    val etlSql =
      s"""
        |insert overwrite table dwd.dwd_user_area
        |select product_id,company,max(remote_addr),max(country),max(province),max(city),
        |max(location),active_user,count_date from dws.dws_uv_total where nvl(active_user,'')!='' and country='中国' and product_id not in ('300','301','302')
        |group by product_id,company,active_user,count_date
      """.stripMargin

    println(etlSql)

    spark.sql(etlSql)

  }

}
