package com.atguigu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL09_Save {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkSQL").setMaster("local[*]")

    conf.set("spark.sql.sources.default","json")

    //2.创建SparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = spark.read.json("input/user.json")

    //保存数据
    //特殊保存
//    df.write.json("output/user.json")

    //通用保存
//    df.write.save("output/user1.json") //parquet格式

    // format指定保存数据类型
//    df.write.format("json").save("output/user2.json")

    //在conf上set文件资源的默认值为json
    df.write.save("output/user3.json")


    //4.关闭ss
    spark.stop()
  }
}
