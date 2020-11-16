package com.pep.dim

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 将172.30.0.9上Mysql中user_db库中的p_user全量业务用户表导入数仓的ods
  * 层的ods_product_user的put_date=20190000分区中
  */
object DimRegionData2DW {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("RUN-DimRegionData2DW")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    /*val props = new java.util.Properties
    val tableName = "a_region_data"
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
    ).map(x => s"mod(region_code,10)=${x}")

    val mysqlReadDF: DataFrame = spark.read.format("jdbc").jdbc(props.getProperty("url"),tableName,predicates,props)
*/
    val createSql =
      s"""
         |create table if not exists dim.dim_region(
         |region_code           string,
         |province              string,
         |city                  string,
         |region                string,
         |level                 string
         |)
         |STORED AS parquet
      """.stripMargin
    spark.sql(createSql)
    val querySql =
      s"""
        |select *
        |from
        |(select *,row_number() over(partition by region_code order by region_code)
        |as rn from ods.ods_region_data ) t where t.rn=1
      """.stripMargin
    spark.sql(querySql).createOrReplaceTempView("a_region_data")

    //将临时表写入数仓
    val etlSql =
      s"""
         |select t.region_code,t.province,t.city,t.region,t.level from (
         |select a.region_code as region_code,'3' as level ,a.region_name as region,b.region_name as city,c.region_name as province
         |from a_region_data a join a_region_data b on a.parent_code=b.region_code
         |join a_region_data c on b.parent_code=c.region_code where a.is_leaf='0' and a.level='3'
         |union all
         |select a.region_code as region_code,'2' as level , '' as region,a.region_name as city,b.region_name as province
         |from a_region_data a join a_region_data b on a.parent_code=b.region_code where a.is_leaf='0' and a.level='2'
         |union all
         |select a.region_code as region_code,'2' as level , '' as region,a.region_name as city,b.region_name as province
         |from a_region_data a join a_region_data b on a.parent_code=b.region_code where a.is_leaf='1' and a.level='2'
         |union all
         |select a.region_code as region_code,'1' as level , '' as region,'' as city,a.region_name as province
         |from a_region_data a where a.level='1'
         |) t order by region_code
       """.stripMargin

    val etlDF: DataFrame = spark.sql(etlSql)
    etlDF.show(10)
    val writeDF = etlDF.coalesce(1)
    writeDF.write.mode("overwrite").parquet("hdfs://emr-cluster/user/hive/warehouse/dim.db/dim_region")

    spark.stop()
  }

}
