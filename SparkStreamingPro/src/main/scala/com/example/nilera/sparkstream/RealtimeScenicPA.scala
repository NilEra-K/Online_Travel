package com.example.nilera.sparkstream

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object RealtimeScenicPA {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("RealtimeScenic").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        val streamingContext = new StreamingContext(conf, Seconds(5))

        val kafkaParams = Map[String, Object] (
            ""->"",
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

        val updateFunc = (newValues: Seq[Int], state: Option[Int]) => {
            val currentCount = newValues.sum
            val previousCount = state.getOrElse(0)
            Some(currentCount + previousCount)
        }

        // 1. 统计相同经纬度和时间的人流量数据
        val peopleCountPerLocationAndTime: DStream[(String, Int)] = kafkaStream.map(_.value)
          .map(line => {
              val splitData = line.split(",")
              (s"${splitData(0)},${splitData(1)},${splitData(2)},${splitData(3)}", 1)
          })
          .updateStateByKey(updateFunc)

        // 2. 基于状态操作统计每个景点每分钟的人流量
        val peopleCountPerMinutePerLocation = peopleCountPerLocationAndTime.map(line => {
            val keySplit = line._1.split(",")
            (s"${keySplit(2)},${keySplit(3).substring(0, 12)}", line._2)
        })

        // 输出结果
        peopleCountPerLocationAndTime.print()
        peopleCountPerMinutePerLocation.print()

        // 启动 StreamingContext
        streamingContext.start()

        // 阻塞 StreamingContext
        streamingContext.awaitTermination()
    }
}
