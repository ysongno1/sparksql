package com.atguigu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession, functions}

/*
    用户自定义函数
    UDAF：输入多行，返回一行。
 */

object SparkSQL06_UDAF {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkSQL").setMaster("local[*]")

    //2.创建SparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = spark.read.json("input/user.json")

    df.createTempView("user")

    spark.udf.register("MyAVG",functions.udaf(new MyAVG))

    spark.sql("select MyAVG(age) from user").show()

    //4.关闭ss
    spark.stop()
  }

  //样例类默认的参数是val 修改为var
  case class Buff(var sum: Long, var cnt: Long)

  /**
   * 输入:Long
   * 缓存区:Buff
   * 输出:Double
   */
  class MyAVG extends Aggregator[Long, Buff, Double] {
    //缓存区初始化方法
    override def zero: Buff = Buff(0L, 0L)

    //缓存区在分区内的聚合方法
    override def reduce(buff: Buff, age: Long): Buff = {
      buff.sum += age
      buff.cnt += 1
      buff
    }

    //多个缓存区在分区间的聚合方法
    override def merge(b1: Buff, b2: Buff): Buff = {
      b1.sum += b2.sum
      b1.cnt += b2.cnt
      b1
    }

    //最终的逻辑计算方法(求平均值的方法)
    override def finish(reduction: Buff): Double = {
      reduction.sum.toDouble / reduction.cnt
    }

    //buff和最终输出结果的序列化方法
    override def bufferEncoder: Encoder[Buff] = Encoders.product

    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }

}
