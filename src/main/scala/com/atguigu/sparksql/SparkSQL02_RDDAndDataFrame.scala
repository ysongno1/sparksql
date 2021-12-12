package com.atguigu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SparkSQL02_RDDAndDataFrame {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkSQL").setMaster("local[*]")

    //2.创建SparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val lineRDD: RDD[String] = spark.sparkContext.textFile("input/user.txt")

    //普通RDD
    val rdd: RDD[(String, String)] = lineRDD.map(line => {
      val datas: Array[String] = line.split(",")
      (datas(0), datas(1))
    })

    //样例类RDD
    val sampleRDD: RDD[User] = rdd.map({
      case (name, age) => User(name, age.toLong)
    })



    //导入隐式转换
    import spark.implicits._

    //rdd -> df
    val df: DataFrame = rdd.toDF("name", "age")
    df.show()

    //df -> rdd
    df.rdd.collect().foreach(println)

    //样例类rdd -> df
    val df1: DataFrame = sampleRDD.toDF()
    df1.show()

    //df -> rdd
    val rdd1: RDD[Row] = df1.rdd

    rdd1.map(row =>{
      (row.getString(0),row.getLong(1))
    }).collect().foreach(println)

    //4.关闭ss
    spark.stop()
  }
}

case class User(name: String, age: Long)