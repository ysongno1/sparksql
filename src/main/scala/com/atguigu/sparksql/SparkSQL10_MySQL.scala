package com.atguigu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL10_MySQL {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkSQL").setMaster("local[*]")

    //2.创建SparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //通用的load方法读取mysql的表数据
    val df: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://hadoop102:3306/gmall")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "user_info")
      .load()

    df.createTempView("user")

    spark.sql("select * from user").show(200,false) //打印完全


    //4.关闭ss
    spark.stop()
  }
}
