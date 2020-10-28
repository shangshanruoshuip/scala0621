package com.atguigu.sparksql01

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

object SparkSQL06_UDAF_1 {

  def main ( args : Array[String] ) : Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkSessinon对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    // 3 读取数据
    val  df: DataFrame = spark.read.json("input/user.json")

    // 4 创建DataFrame临时视图
    df.createOrReplaceTempView("user")
    // 5 注册UDAF
     spark.udf.register("myAvg",functions.udaf(new MyAvgUDAF()))
    // 6 调用自定义UDAF函数
    spark.sql("select myAvg(age)from user").show()

    //4.关闭连接
    spark.stop()
  }

}
//输入数据类型
case class Buff(var sum: Long, var count: Long)

/**
 * 1,20岁； 2,19岁； 3,18岁
 * IN:聚合函数的输入类型：Long
 * BUF：
 * OUT:聚合函数的输出类型:Double  (18+19+20)/3
 */
class MyAvgUDAF extends Aggregator[Long, Buff, Double] {

  // 初始化缓冲区
  override def zero: Buff = Buff(0L, 0L)

  // 将输入的年龄和缓冲区的数据进行聚合
  override def reduce(buff: Buff, age: Long): Buff = {
    buff.sum = buff.sum + age
    buff.count = buff.count + 1
    buff
  }

  // 多个缓冲区数据合并
  override def merge(buff1: Buff, buff2: Buff): Buff = {
    buff1.sum = buff1.sum + buff2.sum
    buff1.count = buff1.count + buff2.count
    buff1
  }

  // 完成聚合操作，获取最终结果
  override def finish(buff: Buff): Double = {
    buff.sum.toDouble / buff.count
  }

  // SparkSQL对传递的对象的序列化操作（编码）
  // 自定义类型就是product   自带类型根据类型选择
  override def bufferEncoder: Encoder[Buff] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}