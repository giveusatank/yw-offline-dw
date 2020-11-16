package com.pep.dwd

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control.Breaks

object DwsUvTotal2DwdUserArea {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RUN-DwsUvTotal2DwdUserArea")
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
    OdsJxwOrganization2DimOrganization(spark, yestodayStr)
    writeDwsUvTotal2DwdUserArea(spark,yestodayStr)
  }
  def OdsJxwOrganization2DimOrganization(spark: SparkSession, yesStr: String): Unit = {
    spark.sql("use dim")
    val createSql =
      """
        |create table if not exists dim.dim_organization(
        |product_id     string,
        |company        string,
        |id             string,
        |reg_name       string,
        |region_code    string,
        |name           string,
        |country        string,
        |province       string,
        |city           string,
        |region         string,
        |level          string,
        |row_timestamp  string,
        |row_status     string
        |)
      """.stripMargin
    spark.sql(createSql)

    val selectSql =
      """
        |insert overwrite table dim.dim_organization
        |select
        |a.product_id     ,
        |a.company        ,
        |a.id             ,
        |a.reg_name       ,
        |a.region_code    ,
        |a.name           ,
        |'中国'            ,
        |b.province       ,
        |b.city           ,
        |b.region         ,
        |b.level          ,
        |a.row_timestamp  ,
        |a.row_status from
        |(select * from ( select *, row_number() over (partition by id order by row_timestamp desc ) num from ods.ods_organization
        |) where num=1 and row_status in ('1','2')) a join dim.dim_region b on a.region_code=b.region_code
      """.stripMargin
    spark.sql(selectSql)
  }

  def writeDwsUvTotal2DwdUserArea(spark: SparkSession, yestodayStr: String) = {

    spark.sql("use dwd")

    val createSql =
      """
        |create table if not exists dwd.dwd_user_area(
        |product_id string,
        |company string,
        |remote_addr string,
        |country string,
        |province string,
        |city string,
        |location string,
        |org_name string,
        |active_user string,
        |count_date string
        |) STORED AS parquet
      """.stripMargin

    spark.sql(createSql)

    val etlSql =
      s"""
        |insert overwrite table dwd.dwd_user_area
        |select
        |a.product_id,
        |a.company,
        |a.remote_addr,
        |nvl(c.country,a.country),
        |nvl(c.province,a.province),
        |nvl(c.city,a.city),
        |nvl(c.region,a.location),
        |nvl(c.reg_name,''),
        |a.active_user,
        |a.count_date
        |from
        |(select * from dws.dws_uv_total where nvl(active_user,'')!='' and country='中国')  a
        |left join (select * from dwd.dwd_product_user where org_id is not null) b
        |on a.active_user=b.user_id and a.product_id=b.product_id and a.company=b.company
        |left join dim.dim_organization c on b.org_id=c.id
      """.stripMargin

    println(etlSql)

    spark.sql(etlSql)

  }

}
