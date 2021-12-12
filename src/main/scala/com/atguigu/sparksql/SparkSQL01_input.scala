package com.atguigu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL01_input {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkSQL").setMaster("local[*]")

    //2.创建SparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()


    val df: DataFrame = spark.read.json("input/user.json")

    df.show()

    //sql语法格式
    df.createTempView("user")
    spark.sql("select * from user where age < 20").show()

    //DSL语法格式
    df.select("name", "age").where("age < 19").show()

    //4.关闭ss
    spark.stop()
  }

}
