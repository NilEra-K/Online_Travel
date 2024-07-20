package com.example.nilera.quickstart

import java.sql.{Connection, PreparedStatement}

import com.example.nilera.utils.KafkaOffsetManagerUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaOffset {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCount").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.port.maxRetries", "100")
        val streamingContext = new StreamingContext(conf, Seconds(5))

        val topics = Array("KafkaOffset")
        val groupId = "szy"

        val kafkaParams = Map[String, Object] (
            ""->"",
            "bootstrap.servers"     -> "bigdata:9092",
            "key.deserializer"      -> classOf[StringDeserializer],
            "value.deserializer"    -> classOf[StringDeserializer],
            "group.id"              -> groupId,
            "auto.offset,reset"     -> "earliest",
            "enable.auto.commit"    -> (false: java.lang.Boolean)       // 自动提交 offset(自动记录读到topic中的哪个部位)
        )


        // 先读取数据库的 Offset, 设置到 Subscribe
        val offset: Map[TopicPartition, Long] = KafkaOffsetManagerUtils.readOffset(topics(0), groupId)
        println(">>> [LOGS] The Offset Now Reading: " + offset)

        val kafkaStream = KafkaUtils.createDirectStream[String, String](
            streamingContext,
            PreferConsistent,
            Subscribe[String, String](topics, kafkaParams, offset)  // 指定订阅的 Topic
        )

        // 测试数据:
        // a,a,a,a,a
        // b,b,b,c
        // c,c,d
        kafkaStream.foreachRDD { rdd =>
            val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            rdd.foreachPartition { iter =>
                val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
                /**
                 * @param: o.topic:        从 topics 可以获取到
                 * @param: o.partition:    分区
                 * @param: o.fromOffset:   正在执行的 Offset
                 * @param: o.untilOffset:  即将执行的 Offset
                 */
                println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
                KafkaOffsetManagerUtils.saveOffset(offsetRanges, groupId)
            }
        }

        kafkaStream.print()

        streamingContext.start()
        streamingContext.awaitTermination()
    }
}
