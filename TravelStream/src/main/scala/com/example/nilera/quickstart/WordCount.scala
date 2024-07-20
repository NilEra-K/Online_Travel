package com.example.nilera.quickstart

import java.sql.{Connection, PreparedStatement}

import com.example.nilera.utils.ConnectionUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object WordCount {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCount").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.port.maxRetries", "100")
        val streamingContext = new StreamingContext(conf, Seconds(5))

        val kafkaParams = Map[String, Object] (
            ""->"",
            "bootstrap.servers"     -> "bigdata:9092",
            "key.deserializer"      -> classOf[StringDeserializer],
            "value.deserializer"    -> classOf[StringDeserializer],
            "group.id"              -> "WordCount",
            "auto.offset,reset"     -> "latest",
            "enable.auto.commit"    -> (false: java.lang.Boolean)       // 自动提交 offset(自动记录读到topic中的哪个部位)
        )

        val topics = Array("WordCount")

        val kafkaStream = KafkaUtils.createDirectStream[String, String](
            streamingContext,
            PreferConsistent,
            Subscribe[String, String](topics, kafkaParams)
        )

        val wcDStream: DStream[(String, Int)] = kafkaStream.flatMap(x=>x.value().split(",")).map((_, 1)).reduceByKey(_ + _)

        // 测试数据：
        // a,b,a,c,a,d,c,c,a,m,e,f
        // (a, 4)
        // (b, 1)
        // (c, 3)
        // (d, 1)
        // (e, 1)
        // (m, 1)
        // (f, 1)
        wcDStream.foreachRDD(rdd =>
            rdd.foreachPartition { partitionOfRecords =>
                val connection: Connection = ConnectionUtils.getConnection()
                val sql = "INSERT INTO wordcount(word, count) VALUES (?, ?)"    // 在执行这个SQL语句之前需要创建 `wordcount` 表
                val pst: PreparedStatement = connection.prepareStatement(sql)
                partitionOfRecords.foreach(record => {
                    pst.setString(1, record._1)
                    pst.setLong(2, record._2)
                    pst.execute()
                })
                if(pst != null) {
                    pst.close()
                }
                ConnectionUtils.closeConnection(connection)
            }
        )

        streamingContext.start()
        streamingContext.awaitTermination()
    }
}
