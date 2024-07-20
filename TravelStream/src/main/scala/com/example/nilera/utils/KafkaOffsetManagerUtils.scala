package com.example.nilera.utils

import java.sql.{Connection, PreparedStatement, ResultSet}

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

import scala.collection.mutable.ListBuffer

object KafkaOffsetManagerUtils {
    // 保存 Offset
    def saveOffset(offsetRanges: Array[OffsetRange], groupId: String) = {
        val connection: Connection = ConnectionUtils.getConnection()
        val sql =
            """
              |INSERT INTO t_offset(k_topic, k_groupid, k_partition, k_offset) VALUES(?, ?, ?, ?)
              |ON DUPLICATE KEY UPDATE k_offset=?
              |""".stripMargin
        val pst: PreparedStatement = connection.prepareStatement(sql)
        offsetRanges.map(offsetRanges => {
            val topic       = offsetRanges.topic
            val partition   = offsetRanges.partition
            val offset      = offsetRanges.untilOffset
            pst.setString(1, topic)
            pst.setString(2, groupId)
            pst.setInt(3, partition)
            pst.setLong(4, offset)
            pst.setLong(5, offset)
            pst.execute()
        })
        ConnectionUtils.closeConnection(connection)
    }

    // 读 Offset
    def readOffset(topic: String, groupId: String): Map[TopicPartition, Long] = {
        val connection: Connection = ConnectionUtils.getConnection()
        val sql = "SELECT k_topic, k_groupid, k_partition, k_offset FROM t_offset WHERE k_topic=? AND k_groupid=?"
        val pst: PreparedStatement = connection.prepareStatement(sql)
        pst.setString(1, topic)
        pst.setString(2, groupId)
        val resultSet: ResultSet = pst.executeQuery()
        val list = new ListBuffer[(TopicPartition, Long)]
        while(resultSet.next()) {
            val partition = resultSet.getInt("k_partition")
            val topicPartition = new TopicPartition(topic, partition)
            val offset = resultSet.getLong("k_offset")
            list.append((topicPartition, offset))
        }
        list.toMap
    }
}
