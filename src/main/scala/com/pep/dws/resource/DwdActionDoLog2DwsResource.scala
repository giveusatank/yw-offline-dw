package com.pep.dws.resource

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.pep.common.Constants
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.control.Breaks

object DwdActionDoLog2DwsResource {

  def writeActionDoLog2DwsResourceUsedDaily(spark: SparkSession, yesStr: String) = {
    spark.sql("use dws")
    val _sql1 =
    """
      |create table if not exists	dws_resource_used_daily
      |(
      |    product_id         string,
      |    company            string,
      |    country            string,
      |    province           string,
      |    city               string,
      |    region             string,
      |    location           string,
      |    user_id            string,
      |    device_id          string,
      |    resource_id        string,
      |    title              string,
      |    tb_id              string,
      |    dzwjlx             string,
      |    dzwjlx_name        string,
      |    ex_zycj            string,
      |    ex_zynrlx          string,
      |    ex_zynrlx_name     string,
      |    file_md5           string,
      |    file_size          bigint,
      |    ori_tree_code      string,
      |    ori_tree_name      string,
      |    zxxkc              string,
      |    nj                 string,
      |    fascicule          string,
      |    rkxd               string,
      |    year               string,
      |    publisher          string,
      |    action_count       bigint
      |) partitioned by (count_date string)
      |    stored as parquet
    """.stripMargin
    spark.sql(_sql1)

    val sql =
    s"""
       |insert overwrite table dws.dws_resource_used_daily partition (count_date)
       |select product_id,
       |       dws.yunwangDateFormat('company',company),
       |       split(max(concat(start_time, '-', country)), '-')[1] as country,
       |       split(max(concat(start_time, '-', province)), '-')[1] as province ,
       |       split(max(concat(start_time, '-', city)), '-')[1] as city,
       |       '' as region,
       |       '' as location,
       |       active_user,
       |       max(device_id),
       |       passive_obj,
       |       b.title,
       |       b.tb_id,
       |       b.dzwjlx,
       |       b.dzwjlx_name,
       |       b.ex_zycj,
       |       b.ex_zynrlx,
       |       b.ex_zynrlx_name,
       |       b.file_md5,
       |       b.file_size,
       |       b.ori_tree_code,
       |       b.ori_tree_name,
       |       dws.getEduCode( b.tb_id, 'zxxkc'),
       |       dws.getEduCode( b.tb_id, 'nj'),
       |       dws.getEduCode( b.tb_id, 'fascicule'),
       |       dws.getEduCode( b.tb_id, 'rkxd'),
       |       dws.getEduCode( b.tb_id, 'year'),
       |       dws.getEduCode( b.tb_id, 'publisher'),
       |       count(1),
       |       t.put_date
       |from dwd.action_do_log t join dwd.dwd_resource_jxw b on t.passive_obj=b.id
       |where t.put_date = '$yesStr'
       |  and action_title in ('jx200305', 'jx200057', 'jx200307', 'jx200154')
       |  and country='中国'
       |group by product_id,
       | dws.yunwangDateFormat('company',company),country,province,city,
       | active_user,
       | passive_obj,
       | b.title,
       | b.tb_id,
       | b.tb_id,
       | b.dzwjlx,
       | b.dzwjlx_name,
       | b.ex_zycj,
       | b.ex_zynrlx,
       | b.ex_zynrlx_name,
       | b.file_md5,
       | b.file_size,
       | b.ori_tree_code,
       | b.ori_tree_name,
       | t.put_date
     """.stripMargin

    spark.sql(sql)
  }

  /**
         insert overwrite table dws.dws_resource_used_total partition (count_date='20200908')
         select
         product_id        ,
         company           ,
         split(max(concat(count_date, '-', country)), '-')[1] as country           ,
         split(max(concat(count_date, '-', province)), '-')[1] as province          ,
         split(max(concat(count_date, '-', city)), '-')[1] as city              ,
         ''            ,
         ''          ,
         user_id           ,
         split(max(concat(count_date, '-', device_id)), '-')[1]  as device_id         ,
         resource_id       ,
         title             ,
         tb_id             ,
         dzwjlx            ,
         dzwjlx_name       ,
         ex_zycj           ,
         ex_zynrlx         ,
         ex_zynrlx_name    ,
         file_md5          ,
         file_size         ,
         ori_tree_code     ,
         ori_tree_name     ,
         zxxkc             ,
         nj                ,
         fascicule         ,
         rkxd              ,
         year              ,
         publisher         ,
         sum(action_count)  from
         (select *
         from dws.dws_resource_used_daily )
         group by product_id,
         company,
         user_id,
         resource_id,
         title,
         tb_id,
         dzwjlx,
         dzwjlx_name,
         ex_zycj,
         ex_zynrlx,
         ex_zynrlx_name,
         file_md5,
         file_size,
         ori_tree_code,
         ori_tree_name,
         zxxkc             ,
         nj                ,
         fascicule         ,
         rkxd              ,
         year              ,
         publisher         ;

    */

  def writeActionDoLog2DwsResourceUsedTotal(spark: SparkSession, yesStr: String ,theDayBeforeYesterday:String) = {
    spark.sql("use dws")
    val _sql1 =
      """
        |create table if not exists	dws_resource_used_total
        |(
        |    product_id         string,
        |    company            string,
        |    country            string,
        |    province           string,
        |    city               string,
        |    region             string,
        |    location           string,
        |    user_id            string,
        |    device_id          string,
        |    resource_id        string,
        |    title              string,
        |    tb_id              string,
        |    dzwjlx             string,
        |    dzwjlx_name        string,
        |    ex_zycj            string,
        |    ex_zynrlx          string,
        |    ex_zynrlx_name     string,
        |    file_md5           string,
        |    file_size          bigint,
        |    ori_tree_code      string,
        |    ori_tree_name      string,
        |    zxxkc              string,
        |    nj                 string,
        |    fascicule          string,
        |    rkxd               string,
        |    year               string,
        |    publisher          string,
        |    action_count       bigint
        |) partitioned by (count_date string)
        |    stored as parquet
      """.stripMargin
    spark.sql(_sql1)

    val sql =
      s"""
         |insert overwrite table dws.dws_resource_used_total partition (count_date)
         |select
         |product_id        ,
         |company           ,
         |split(max(concat(count_date, '-', country)), '-')[1] as country       ,
         |split(max(concat(count_date, '-', province)), '-')[1] as province     ,
         |split(max(concat(count_date, '-', city)), '-')[1] as city             ,
         |''            ,
         |''          ,
         |user_id           ,
         |split(max(concat(count_date, '-', device_id)), '-')[1]  as device_id  ,
         |resource_id       ,
         |title             ,
         |tb_id             ,
         |dzwjlx            ,
         |dzwjlx_name       ,
         |ex_zycj           ,
         |ex_zynrlx         ,
         |ex_zynrlx_name    ,
         |file_md5          ,
         |file_size         ,
         |ori_tree_code     ,
         |ori_tree_name     ,
         |zxxkc             ,
         |nj                ,
         |fascicule         ,
         |rkxd              ,
         |year              ,
         |publisher         ,
         |sum(action_count) ,
         |'$yesStr'
         |from
         |(select * from dws.dws_resource_used_daily where count_date='$yesStr'
         |UNION ALL
         | select * from dws.dws_resource_used_total where count_date='$theDayBeforeYesterday' )
         |group by product_id,
         |company,
         |user_id,
         |resource_id,
         |title,
         |tb_id,
         |dzwjlx,
         |dzwjlx_name,
         |ex_zycj,
         |ex_zynrlx,
         |ex_zynrlx_name,
         |file_md5,
         |file_size,
         |ori_tree_code,
         |ori_tree_name,
         |zxxkc             ,
         |nj                ,
         |fascicule         ,
         |rkxd              ,
         |year              ,
         |publisher
     """.stripMargin
    spark.sql(sql)
  }

  def doAction(spark: SparkSession, yesStr: String, todayStr: String, _7DaysBefore: String,theDayBeforeYesterday:String) = {
    writeActionDoLog2DwsResourceUsedDaily(spark, yesStr)
    writeActionDoLog2DwsResourceUsedTotal(spark, yesStr, theDayBeforeYesterday)
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("JOB-OdsActionDoLog2DwsTextBook").set("spark.sql.shuffle.partitions", Constants.dws_shuffle_partitions)
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val break = new Breaks
    //标记为判断是否带参
    var withParams = true
    if(args.length == 0) withParams = false
    val format = new SimpleDateFormat("yyyyMMdd")
    break.breakable {
      //如果传参了就是循环参数个遍数
      for(i <- 0 until (if (args.length>0) args.length else 1)){
        //判断传入的参数是否合法，如果不是8位数字就跳出循环
        if(withParams){
          val reg = "^[0-9]{8}$".r
          if(None == reg.findPrefixOf(args(i))) break.break()
        }
        var todayStr = format.format(new Date())
        if(withParams) todayStr = format.format(format.parse(args(i)))
        val cal = Calendar.getInstance
        cal.setTime(format.parse(todayStr))
        cal.add(Calendar.DATE, -1)
        if (withParams) {
          //按参数执行，执行参数当天的
          cal.add(Calendar.DATE, 1)
        }
        val yesStr: String = format.format(cal.getTime)
        cal.add(Calendar.DATE, -1)
        val theDayBeforeYesterday: String = format.format(cal.getTime)
        cal.add(Calendar.DATE, -5)
        val _7DaysBefore: String = format.format(cal.getTime)
        doAction(spark,yesStr,todayStr,_7DaysBefore,theDayBeforeYesterday)
      }
    }
    spark.stop()
  }
}
