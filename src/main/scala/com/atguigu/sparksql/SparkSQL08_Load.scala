package com.atguigu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL08_Load {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkSQL").setMaster("local[*]")

    conf.set("spark.sql.sources.default","json")

    //2.创建SparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //加载数据
    //特殊加载
    val df: DataFrame = spark.read.json("input/user.json")
    df.show()

    val df2: DataFrame = spark.read.csv("input/user.txt")
    df2.show()

    //通用加载
//    val df3: DataFrame = spark.read.load("input/user.json")
//    df3.show()
    //报错，默认通用加载格式是Parquet

    //format指定加载数据类型
    spark.read.format("json").load("input/user.json").show()

    //在conf上set文件资源的默认值为json
    spark.read.load("input/user.json").show()

    //4.关闭ss
    spark.stop()
  }
}
