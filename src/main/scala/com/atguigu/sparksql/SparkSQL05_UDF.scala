package com.atguigu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
    用户自定义函数
    UDF：一行进入，一行出
 */

object SparkSQL05_UDF {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkSQL").setMaster("local[*]")

    //2.创建SparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = spark.read.json("input/user.json")

    //创建DataFrame临时试图
    df.createTempView("user")

    //注册UDF函数。功能：在数据前添加字符串“Name:”
    spark.udf.register("addNAme", (name: String) => "Name:" + name)
    spark.udf.register("doubleAge", (age: Long) => age * 2)

    //调用自定义UDF函数
    spark.sql("select addName(name), age from user").show()
    spark.sql("select addName(name), doubleAge(age) from user").show()

    //4.关闭ss
    spark.stop()
  }
}
