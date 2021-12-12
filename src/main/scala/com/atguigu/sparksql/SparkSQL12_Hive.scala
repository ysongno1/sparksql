package com.atguigu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSQL12_Hive {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME","atguigu")

    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkSQL").setMaster("local[*]")

    //2.创建SparkSession
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()


    spark.sql("show databases").show()


    //4.关闭ss
    spark.stop()
  }
}
