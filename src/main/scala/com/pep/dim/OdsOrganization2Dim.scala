package com.pep.dim

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.control.Breaks

object OdsOrganization2Dim {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RUN-OdsOrganization2Dim")
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


}
