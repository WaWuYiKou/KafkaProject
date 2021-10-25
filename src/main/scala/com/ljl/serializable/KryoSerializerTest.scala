package com.ljl.serializable

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.sql.SparkSession

object KryoSerializerTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("KryoSerializerTest")
    // 配置KryoSerializer序列化
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // 方式一
    sparkConf.set("spark.kryo.registrator", classOf[MyKryoRegistrator].getName)
    // 方式二
    //sparkConf.registerKryoClasses(Array(classOf[User], classOf[scala.collection.mutable.WrappedArray.ofRef[_]]))
    sparkConf.set("spark.kryo.registrationRequired", "true")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val orgRDD = sc.parallelize(Seq(
      User(1, "cc", "bj"),
      User(2, "aa", "bj"),
      User(3, "qq", "bj"),
      User(4, "pp", "bj")
    ))
    spark.createDataFrame(orgRDD).show()

    spark.stop()
  }
  case class User(id: Int, name: String, city: String)
  class MyKryoRegistrator extends KryoRegistrator {
    override def registerClasses(kryo: Kryo): Unit = {
      kryo.register(classOf[User])
    }
  }
}
