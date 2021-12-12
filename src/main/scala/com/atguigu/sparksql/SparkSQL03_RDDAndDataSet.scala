package com.atguigu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object SparkSQL03_RDDAndDataSet {
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

    //rdd ->ds
    val ds: Dataset[(String, String)] = rdd.toDS() //不支持写入列名
    ds.show()

    //ds -> rdd
    ds.rdd.collect().foreach(println)

    //样例类 -> ds
    val ds2: Dataset[User] = sampleRDD.toDS()
    ds2.show()

    //ds -> rdd
    ds2.rdd.collect().foreach(println)

    //4.关闭ss
    spark.stop()
  }
}
