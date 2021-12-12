package com.atguigu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkSQL04_DataFrameAndDataSet {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkSQL").setMaster("local[*]")

    //2.创建SparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = spark.read.json("input/user.json")

    df.show()

    //导入隐式转换
    import spark.implicits._

    //todo DF=>DS
    val ds: Dataset[User] = df.as[User]

    ds.show()

    //todo DS => DF

    val df1: DataFrame = ds.toDF()

    df1.show()

    //4.关闭ss
    spark.stop()
  }
}
