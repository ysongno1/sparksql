package com.atguigu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkSQL11_MySQL_Write {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkSQL").setMaster("local[*]")

    //2.创建SparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = spark.read.json("input/user.json")

    //当表的字段名和json文件的字段名不一样的时候 可以
    //df.toDF("name","age") 修改相同的字段名

    df.write.format("jdbc")
      .option("url", "jdbc:mysql://hadoop102:3306/test")
      //.option("url", "jdbc:mysql://localhost:3306/db1")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      //.option("password", "root")
      .option("dbtable", "user")
      .mode(SaveMode.Append)
      .save()

    //4.关闭ss
    spark.stop()
  }
}
