package com.ljl.receiver

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

import java.io.BufferedReader
import java.net.Socket
import java.nio.charset.StandardCharsets
import scala.tools.jline_embedded.internal.InputStreamReader

class CustomReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {
  override def onStart(): Unit = {
    // Start the thread that receives data over a connection
    new Thread("Socket Receiver") {
      override def run(): Unit = {
          receive()
      }
    }.start()
  }

  override def onStop(): Unit = {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself isStopped() returns false
  }

  // 创建一个socket连接
  private def receive() = {
    var socket: Socket = null
    var userInput: String = null
    try {
      println(s"Connecting to $host: $port")
      socket = new Socket(host, port)
      println(s"Connected to $host: $port")
      val reader = new BufferedReader(
        new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8)
      )
      userInput = reader.readLine()
      while (!isStopped() && userInput != null) {
        store(userInput)
        userInput = reader.readLine()
      }
      reader.close()
      socket.close()
      println("Stopped receiving")
      restart("Trying to connect again")
    } catch {
      case e: java.net.ConnectException => restart(s"Error connecting to $host: $port", e)
      case t: Throwable => restart("Error receiving data", t)
    }
  }
}

object CustomReceiver {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: CustomReceiver <hostname> <port>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("CustomReceiver")
    // 配置KryoSerializer序列化后，接收器接收到的数据是否也会使用该序列化存储到executor中呢？
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.kryo.registrationRequired", "true")
    sparkConf.registerKryoClasses(Array(classOf[CustomReceiver]))

    val ssc = new StreamingContext(sparkConf, Seconds(3))
    val lines = ssc.receiverStream(new CustomReceiver(args(0), args(1).toInt))
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map((_, 1)).reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
