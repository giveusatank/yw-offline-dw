package com.pep.dws.order

import java.text.SimpleDateFormat
import java.util.regex.{Matcher, Pattern}
import java.util.{Calendar, Date}

import com.pep.common.DbProperties
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.control.Breaks
import scala.util.matching.Regex


/**
  * 保留每天、最近7天、最近30天的用户注册、用户购买的转化明细
  */
object OdsRegisterAndOrder2DwsRegOrderUserDetails {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("OdsRegisterAndOrder2DwsRegOrderUserDetails")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val withParam = if (args.length > 0) true else false
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val cl = Calendar.getInstance()
    cl.setTime(new Date)
    cl.add(Calendar.DATE, -1)
    var yesStr = sdf.format(cl.getTime)
    val stop = if (withParam) args.length else 1
    val loop = new Breaks
    val regexp = "^\\d{8}$".r

    println("日期为:" + yesStr)

    loop.breakable {
      for (index <- 0 until stop) {

        if (withParam) {
          if (regexp.findPrefixOf(args(index)).isDefined) {
            yesStr = args(index)
          } else {
            loop.break()
          }
        }
        write2AdsRegOrderUserDaily(spark, yesStr)
        write2PG(spark, yesStr)
      }
    }
    spark.stop()
  }

  //每日、最近7天、最近30天的C端用户注册购买转化率统计
  def write2AdsRegOrderUserDaily(spark: SparkSession, yesStr: String) = {

    val sdf1 = new SimpleDateFormat("yyyyMMdd")
    val sdf2 = new SimpleDateFormat("yyyy-MM-dd")
    val yesStrFormat = sdf2.format(sdf1.parse(yesStr))

    //yesStr是自动调度的昨天或者传入日期当天
    val dayMark = "day"
    val weekMark = "week"
    val monthMark = "month"

    val createSql =
      """
        |create table if not exists ads.ads_reg_order_user_convert(
        |product_id string comment '产品Id',
        |reg_count int comment '注册UV',
        |pur_count int comment '订单UV',
        |reg_pur_ratio double comment '转化率' ,
        |date_mark string comment '日期标识',
        |count_date string comment '日期'
        |)
        |stored as parquet
        |comment '用户注册购买转换表'
      """.stripMargin

    spark.sql(createSql)

    val queryDaySql =
      s"""
         |with t1 as (
         |select
         |'1213' as product_id,
         |count(1) as reg_count,
         |sum(if(t2.userId is not null,1,0)) as pur_count,
         |round(sum(if(t2.userId is not null,1,0))/count(1) * 100,2) as reg_pur_ratio,
         |'${dayMark}' as date_mark
         |from  (select distinct(split(user_id,'_')[1]) as userId from ods.ods_product_user where put_date='${yesStr}' and product_id ='1213' ) t1  left join
         |(select distinct(user_id) as userId from ods.ods_order_info where count_date='${yesStr}' and  app_id='44' ) t2
         |on t1.userId = t2.userId ),t2 as (
         |select
         |'1214' as product_id,
         |count(1) as reg_count,
         |sum(if(t2.userId is not null,1,0)) as pur_count ,
         |round(sum(if(t2.userId is not null,1,0))/count(1) * 100,2) as reg_pur_ratio,
         |'${dayMark}' as date_mark
         |from  (select distinct(user_id) as userId from ods.ods_product_user where put_date='${yesStr}'  and company ='pep_click' ) t1  left join
         |(select distinct(user_id) as userId from ods.ods_order_info where count_date='${yesStr}' and app_id='44' ) t2
         |on t1.userId = t2.userId )
         |select *,${yesStr} as count_date from t1
         |union all
         |select *,${yesStr} from t2
      """.stripMargin

    val queryWeekSql =
      s"""
         |with t1 as (
         |select
         |'1213' as product_id,
         |count(1) as reg_count,
         |sum(if(t2.userId is not null,1,0)) as pur_count ,
         |round(sum(if(t2.userId is not null,1,0))/count(1) * 100,2) as reg_pur_ratio,
         |'${weekMark}' as date_mark
         |from  (select distinct(split(user_id,'_')[1]) as userId from ods.ods_product_user where put_date between date_format(date_sub('${yesStrFormat}',6),'yyyyMMdd') and date_format(date_sub('${yesStrFormat}',0),'yyyyMMdd')  and product_id ='1213' ) t1  left join
         |(select distinct(user_id) as userId from ods.ods_order_info where count_date between date_format(date_sub('${yesStrFormat}',6),'yyyyMMdd') and date_format(date_sub('${yesStrFormat}',0),'yyyyMMdd')  and  app_id='44' ) t2
         |on t1.userId = t2.userId ),t2 as (
         |select
         |'1214' as product_id,
         |count(1) as reg_count,
         |sum(if(t2.userId is not null,1,0)) as pur_count ,
         |round(sum(if(t2.userId is not null,1,0))/count(1) * 100,2) as reg_pur_ratio,
         |'${weekMark}' as date_mark
         |from  (select distinct(user_id) as userId from ods.ods_product_user where put_date between date_format(date_sub('${yesStrFormat}',6),'yyyyMMdd') and date_format(date_sub('${yesStrFormat}',0),'yyyyMMdd')  and company ='pep_click' ) t1  left join
         |(select distinct(user_id) as userId from ods.ods_order_info where count_date between date_format(date_sub('${yesStrFormat}',6),'yyyyMMdd') and date_format(date_sub('${yesStrFormat}',0),'yyyyMMdd')  and  app_id='44' ) t2
         |on t1.userId = t2.userId )
         |select *,${yesStr} as count_date from t1
         |union all
         |select *,${yesStr} from t2
      """.stripMargin


    val queryMonthSql =
      s"""
         |with t1 as (
         |select
         |'1213' as product_id,
         |count(1) as reg_count,
         |sum(if(t2.userId is not null,1,0)) as pur_count ,
         |round(sum(if(t2.userId is not null,1,0))/count(1) * 100,2) as reg_pur_ratio,
         |'${monthMark}' as date_mark
         |from  (select distinct(split(user_id,'_')[1]) as userId from ods.ods_product_user where put_date between date_format(date_sub('${yesStrFormat}',29),'yyyyMMdd') and date_format(date_sub('${yesStrFormat}',0),'yyyyMMdd')  and product_id ='1213' ) t1  left join
         |(select distinct(user_id) as userId from ods.ods_order_info where count_date between date_format(date_sub('${yesStrFormat}',29),'yyyyMMdd') and date_format(date_sub('${yesStrFormat}',0),'yyyyMMdd')  and  app_id='44' ) t2
         |on t1.userId = t2.userId ),t2 as (
         |select
         |'1214' as product_id,
         |count(1) as reg_count,
         |sum(if(t2.userId is not null,1,0)) as pur_count ,
         |round(sum(if(t2.userId is not null,1,0))/count(1) * 100,2) as reg_pur_ratio,
         |'${monthMark}' as date_mark
         |from  (select distinct(user_id) as userId from ods.ods_product_user where put_date between date_format(date_sub('${yesStrFormat}',29),'yyyyMMdd') and date_format(date_sub('${yesStrFormat}',0),'yyyyMMdd')  and company ='pep_click' ) t1  left join
         |(select distinct(user_id) as userId from ods.ods_order_info where count_date between date_format(date_sub('${yesStrFormat}',29),'yyyyMMdd') and date_format(date_sub('${yesStrFormat}',0),'yyyyMMdd')  and  app_id='44' ) t2
         |on t1.userId = t2.userId )
         |select *,${yesStr} as count_date from t1
         |union all
         |select *,${yesStr} from t2
      """.stripMargin


    spark.sql(queryDaySql).createOrReplaceTempView("temp_day")
    spark.sql(queryWeekSql).createOrReplaceTempView("temp_week")
    spark.sql(queryMonthSql).createOrReplaceTempView("temp_month")

    val insertSql =
      s"""
         |insert into table ads.ads_reg_order_user_convert
         |select * from temp_day
         |union all
         |select * from temp_week
         |union all
         |select * from temp_month
      """.stripMargin

    spark.sql(insertSql)

  }


  def write2PG(spark: SparkSession, yesStr: String) = {
    val props = DbProperties.propScp
    props.setProperty("tableName_1", "ads_reg_order_user_convert")
    props.setProperty("write_mode", "Append")

    //ads_reg_order_user_convert
    val querySql_1 =
      s"""
         |select count_date,reg_count,pur_count,product_id,reg_pur_ratio,date_mark
         |from ads.ads_reg_order_user_convert where count_date='${yesStr}'
      """.stripMargin

    spark.sql(querySql_1).coalesce(5).write.mode(props.getProperty("write_mode")).
      jdbc(props.getProperty("url"), props.getProperty("tableName_1"), props)

  }

}