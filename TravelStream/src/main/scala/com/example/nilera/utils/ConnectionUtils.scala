package com.example.nilera.utils

import java.io.InputStream
import java.sql.Connection
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource

object ConnectionUtils {
    // 1. 创建 Druid 的 DataSource 对象
    val dataSource: DataSource = {
        val properties = new Properties()
        val inputStream: InputStream = getClass.getClassLoader.getResourceAsStream("druid.properties")
        properties.load(inputStream)
        println(properties)
        DruidDataSourceFactory.createDataSource(properties)
    }

    // 2. 创建获取连接的方法, 向外提供数据库连接
    def getConnection(): Connection = {
        dataSource.getConnection()
    }

    // 3. 创建连接回收方法
    def closeConnection(connection: Connection): Unit = {
        if(null != connection) {
            connection.close()
        }
    }

    def main(args: Array[String]): Unit = {
        println(dataSource)
    }
}
