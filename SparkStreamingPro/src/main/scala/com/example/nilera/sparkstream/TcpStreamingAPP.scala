package com.example.nilera.sparkstream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TcpStreamingAPP {
    def main(args: Array[String]): Unit = {
        // 第一步: 创建 StreamingContext
        val conf = new SparkConf().setMaster("local[2]").setAppName("TcpStreaming")
        val streamingContext = new StreamingContext(conf, Seconds(1))

        // 第二步: 读取数据: 接收 hostname:port 发送的数据
        // SparkCore        SparkContext        RDD(弹性分布式数据集)——不可变的、可分区的、可并行计算
        // SparkSQL         SparkSession        DataSet/DataFrame
        // SparkStreaming   StreamingContext    DStream
        val hostname = "bigdata"
        val port     = 9999
        var tcpDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream(hostname, port)

        // 第三步: 处理数据
        val wcDStream: DStream[(String, Int)] = tcpDStream.flatMap(_.split(","))
          .map((_, 1))
          .reduceByKey(_ + _)   // 默认计算的是当前批次的数据, 不会与之前的批次进行累加操作

        // 第四步: 累加结果
        wcDStream.print()

        // 第五步: 启动程序
        streamingContext.start()

        // 第六步: 等待程序关闭
        streamingContext.awaitTermination()
    }
}
