package com.example.nilera.travel

import java.sql.{Connection, PreparedStatement}

import com.example.nilera.utils.{ConnectionUtils, KafkaOffsetManagerUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object RealtimeScenicPA {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("RealtimeScenic").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.port.maxRetries", "100")
        val streamingContext = new StreamingContext(conf, Seconds(5))
        streamingContext.checkpoint("ckpt")

        val topics  = Array("subowen")
        val groupId = "subowen"

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
            Subscribe[String, String](topics, kafkaParams, offset)
        )

        val updateFunc = (newValues: Seq[Int], state: Option[Int]) => {
            val currentCount = newValues.sum
            val previousCount = state.getOrElse(0)
            val newCount = currentCount + previousCount
            Some(newCount)
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
        }).reduceByKey(_ + _)

        // 输出结果
        peopleCountPerLocationAndTime.foreachRDD(rdd =>
            rdd.foreachPartition { partitionOfRecords =>
                val connection: Connection = ConnectionUtils.getConnection()
                val sql =
                    """
                      |INSERT INTO t_heat(longitude, latitude, scenic, sec_moment, sec_quantity)
                      |VALUES (?, ?, ?, ?, ?)
                      |ON DUPLICATE KEY
                      |UPDATE sec_quantity = ?""".stripMargin    // 在执行这个SQL语句之前需要创建表
                val pst: PreparedStatement = connection.prepareStatement(sql)
                partitionOfRecords.foreach(record => {
                    val splitRecord = record._1.split(",")
                    // println(splitRecord(0) + " " + splitRecord(1) + " " + splitRecord(2) + " " + splitRecord(3))
                    pst.setDouble(1, splitRecord(0).toDouble)
                    pst.setDouble(2, splitRecord(1).toDouble)
                    pst.setString(3, splitRecord(2))
                    pst.setString(4, splitRecord(3))
                    pst.setInt(5, record._2)
                    pst.setInt(6, record._2)
                    pst.execute()
                })
                if(pst != null) {
                    pst.close()
                }
                ConnectionUtils.closeConnection(connection)
            }
        )

        peopleCountPerMinutePerLocation.foreachRDD(rdd =>
            rdd.foreachPartition { partitionOfRecords =>
                val connection: Connection = ConnectionUtils.getConnection()
                val sql =
                    """
                      |INSERT INTO t_scenic(scenic, min_moment, min_quantity)
                      |VALUES (?, ?, ?)
                      |ON DUPLICATE KEY
                      |UPDATE min_quantity = ?""".stripMargin    // 在执行这个SQL语句之前需要创建表
                val pst: PreparedStatement = connection.prepareStatement(sql)
                partitionOfRecords.foreach(record => {
                    val splitRecord = record._1.split(",")
                    // println(splitRecord(0) + " " + splitRecord(1) + " " + splitRecord(2) + " " + splitRecord(3))
                    pst.setString(1, splitRecord(0))
                    pst.setString(2, splitRecord(1))
                    pst.setInt(3, record._2)
                    pst.setInt(4, record._2)
                    pst.execute()
                })
                if(pst != null) {
                    pst.close()
                }
                ConnectionUtils.closeConnection(connection)
            }
        )

        // 维护 KafkaStream 的 Offset 表
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

        // peopleCountPerLocationAndTime.print()
        peopleCountPerMinutePerLocation.print()

        // 启动 StreamingContext
        streamingContext.start()

        // 阻塞 StreamingContext
        streamingContext.awaitTermination()
    }
}
