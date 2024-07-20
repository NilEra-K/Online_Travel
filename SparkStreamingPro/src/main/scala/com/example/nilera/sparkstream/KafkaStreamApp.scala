package com.example.nilera.sparkstream

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


object KafkaStreamApp {
    def main(args: Array[String]): Unit = {
        // 第一步
        // Spark 中遇到的一切序列化问题都需要 KryoSerializer
        val conf: SparkConf = new SparkConf().setMaster("Local[2]").setAppName("KafkaStreaming").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        val streamingContext = new StreamingContext(conf, Seconds(5))

        val kafkaParams: Map[String, Object] = Map[String, Object] (
            "" -> "",
            "bootstrap.servers"     -> "bigdata:9092",
            "key.deserializer"      -> classOf[StringDeserializer],
            "value.deserializer"    -> classOf[StringDeserializer],
            "group.id"              -> "subowen",
            "auto.offset,reset"     -> "latest",
            "enable.auto.commit"    -> (false: java.lang.Boolean)       // 自动提交 offset(自动记录读到topic中的哪个部位)
        )

        val topics = Array("subowen")

        val kafkaStream = KafkaUtils.createDirectStream[String, String](
            streamingContext,
            PreferConsistent,
            Subscribe[String, String](topics, kafkaParams)
        )

        kafkaStream.flatMap(x=>x.value().split(",")).map((_, 1)).reduceByKey(_ + _).print()

        // 启动 StreamingContext
        streamingContext.start()

        // 阻塞 StreamingContext
        streamingContext.awaitTermination()
    }
}