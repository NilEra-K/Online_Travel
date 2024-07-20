# Online_Travel
OnlineTravelBigdataPlatform

## 🌳 在线旅游大数据平台项目
<p>
    <label for="file">完成度：</label>
    <progress max="100" value="100"> 100% </progress>
</p>


**日期任务清单**

- [x] 项目开始日期: 2024-06-11
  - [x] 了解项目的背景以及整个系统的架构
  - [x] 了解系统需要完成的主要功能
  - [x] 了解系统整个架构
  - [x] 完成数据服务端的部署
  - [x] 完成数据客户端的部署
  - [x] 了解数据集
  - [x] 认识消息队列 Kafka
  - [x] 完成消息队列 Kafka 的部署
  - [x] 了解消息队列 Kafka 的基本应用
  - [x] 使用 Flume 收集数据到 Kafka
- [x] 2024-06-12
  - [x] 了解实时数据分析所用到的技术
  - [x] 了解 `SparkStreaming` 和 `Flink`
  - [x] 了解 `SparkStreaming` 的核心概念
  - [x] 了解数据源
  - [x] 借助`netcat`实践`Kafka`
  - [x] 了解转换操作
  - [x] 具体实施任务：处理数据

- [x] 2024-06-13 
  - [x] 了解数据库连接池
  - [x] 了解如何向 MySQL 数据库中写入数据
  - [x] 借助 `alibaba Druid` 库实现一个数据库连接工具类 
  - [x] 编写案例：`WordCount`
  - [x] 具体实施任务，将代码中生成的数据写入`MySQL`。

- [x] 2024-06-14
  - [x] 了解什么是 Kafka Offset
  - [x] 维护 Kafka Offset
  - [x] 具体实施任务，将 Kafka 的 Offset 配合 `MySQL` 用代码进行维护。

- [x] 2024-06-17
  - [x] 进行后端开发
  - [x] 进行前端开发

- [x] 2024-06-18
  - [x] 进行热力图的绘制
  - [x] 进行人流量柱状图的绘制
  - [x] 进行人流量趋势图的绘制

- [x] 2024-06-19 项目结束日期

**文件目录**

[TOC]

<!-- more -->

### 📕 1. 项目概述
#### 1.1 项目背景
随着信息技术的飞速发展，旅游行业正迅速融入数字化转型的浪潮中。旅游大数据的产生和积累为行业提供了前所未有的洞察力。然而，传统的数据处理方法往往难以应对数据的海量性和实时性需求。
#### 1.2 项目介绍
本项目旨在构建一个旅游大数据实时分析和监控系统，系统主要包括旅游数据分析和实时监控两大模块，旅游数据分析模块是基于Spark Streaming对济南各景点的人流量数据进行实时处理和分析，实时监控模块是基于SpringBoot对分析的结果进行可视化展示。 

### 🚧 2. 系统功能及架构
#### 2.1 系统主要功能
- 数据实时收集：通过 ***Flume*** 实时采集手机移动信令数据（数据生成器生成的模拟数据），发送到 **Kafka**。
- 数据实时处理分析：通过***Spark Streaming*** 消费 ***Kafka*** 数据，主要完成以下分析：
  - 各景点人流量实时统计（热力图，每秒钟）
  - 各景点人流量随时间增长情况/各景点人流量随时间变化趋势(每分钟)
  - 实时监控：通过 `SpringBoot` + `MyBatis` 构建旅游监控系统，基于高德地图完成每秒钟人流量热力图展示，基于 `Echarts` 完成每分钟流量柱状图和每分钟人流量变化折线图。


#### 2.2 系统结构与技术选型

<img src="https://img-blog.csdnimg.cn/direct/0b6829b9886840a382e4d6e5a18d366b.png#pic_center" alt="SystemStructure" style="zoom: 50%;" />

- **项目开发工具：**`IntelliJ IDEA 2019`

- **数据收集分析**：`Flume` + `Kafka` + `SparkStreaming` + `MySQL/Redis`

- **数据展示：**`SpringBoot` + `MyBatis` + `WebSocket` + `MySQL` + `LayUI` + `Echarts` + `高德地图API`

### 🔧 3.项目收集功能
#### 3.1 数据服务端与数据客户端部署

我的主机信息如下：

```
192.168.26.110      bigdata
192.168.26.111      webserver01
192.168.26.111      webserver02
```

##### 3.1.1 数据服务端部署

1. 将 `logweb-1.0.jar` 上传到服务器`webserver01` 以及 `webserver02`。

2. 启动运行 `logweb` 程序

   ```bash
   nohup java -jar logweb-1.0.jar &
   ```

   运行成功后，查询日志文件结果，结果如下表示正常启动：
   ```
     .   ____          _            __ _ _
    /\\ / ___'_ __ _ _(_)_ __  __ _ \ \ \ \
   ( ( )\___ | '_ | '_| | '_ \/ _` | \ \ \ \
    \\/  ___)| |_)| | | | | || (_| |  ) ) ) )
     '  |____| .__|_| |_|_| |_\__, | / / / /
    =========|_|==============|___/=/_/_/_/
    :: Spring Boot ::               (v2.7.10)
   
   2024-06-11 12:10:18.084  INFO 1288 --- [           main] c.s.i.w.s.web.logweb.LogwebApplication   : Starting LogwebApplication v1.0 using Java 1.8.0_231 on webserver01 with PID 1288 (/home/subowen/serverJar/logweb-1.0.jar started by subowen in /home/subowen/serverJar)
   2024-06-11 12:10:18.090  INFO 1288 --- [           main] c.s.i.w.s.web.logweb.LogwebApplication   : No active profile set, falling back to 1 default profile: "default"
   2024-06-11 12:10:20.604  INFO 1288 --- [           main] o.s.b.w.embedded.tomcat.TomcatWebServer  : Tomcat initialized with port(s): 9527 (http)
   2024-06-11 12:10:20.666  INFO 1288 --- [           main] o.apache.catalina.core.StandardService   : Starting service [Tomcat]
   2024-06-11 12:10:20.666  INFO 1288 --- [           main] org.apache.catalina.core.StandardEngine  : Starting Servlet engine: [Apache Tomcat/9.0.73]
   2024-06-11 12:10:21.301  INFO 1288 --- [           main] o.a.c.c.C.[.[localhost].[/logweb]        : Initializing Spring embedded WebApplicationContext
   2024-06-11 12:10:21.301  INFO 1288 --- [           main] w.s.c.ServletWebServerApplicationContext : Root WebApplicationContext: initialization completed in 3094 ms
   2024-06-11 12:10:23.099  INFO 1288 --- [           main] o.s.b.w.embedded.tomcat.TomcatWebServer  : Tomcat started on port(s): 9527 (http) with context path '/logweb'
   2024-06-11 12:10:23.116  INFO 1288 --- [           main] c.s.i.w.s.web.logweb.LogwebApplication   : Started LogwebApplication in 6.507 seconds (JVM running for 8.07)
   2024-06-11 12:14:14.608  INFO 1288 --- [nio-9527-exec-1] o.a.c.c.C.[.[localhost].[/logweb]        : Initializing Spring DispatcherServlet 'dispatcherServlet'
   2024-06-11 12:14:14.608  INFO 1288 --- [nio-9527-exec-1] o.s.web.servlet.DispatcherServlet        : Initializing Servlet 'dispatcherServlet'
   2024-06-11 12:14:14.609  INFO 1288 --- [nio-9527-exec-1] o.s.web.servlet.DispatcherServlet        : Completed initialization in 1 ms
   ```

##### 3.1.2 数据客户端部署

本次数据客户端直接部署在 **`Windosw`** 下，使用 `IntelliJ IDEA 2019` 进行开发。

`IDEA` 数据客户端结构如下：

<img src="https://img-blog.csdnimg.cn/direct/b7a696fb3de3430c843302479bd8f4ff.png#pic_center" alt="LogClientStruct" style="zoom: 80%;" />

1. 修改 `pom.xml` 引入相应的包

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <parent>
        <artifactId>travel_subowen423</artifactId>
        <groupId>com.example.x.travel</groupId>
        <version>1.0-SNAPSHOT</version>
    </parent>
    <modelVersion>4.0.0</modelVersion>

    <artifactId>logclient</artifactId>

    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <!-- 配置 Spark 的版本 -->
        <spark.version>3.0.1</spark.version>
        <httpclient.version>4.5.12</httpclient.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_2.12</artifactId>
            <version>${spark.version}</version>
        </dependency>

        <dependency>
            <groupId>org.apache.commons</groupId>
            <artifactId>commons-lang3</artifactId>
            <version>3.12.0</version>
        </dependency>

        <dependency>
            <groupId>org.apache.httpcomponents</groupId>
            <artifactId>httpclient</artifactId>
            <version>${httpclient.version}</version>
        </dependency>

        <dependency>
            <groupId>log4j</groupId>
            <artifactId>log4j</artifactId>
            <version>1.2.17</version>
        </dependency>
    </dependencies>
</project>
```

2. 导入 `logclient` 代码，并且修改相应包名和部分代码
3. 执行 `ScenicAPP.java` 随机生成数据。

##### 3.1.3 数据格式说明

| 经度 | 纬度 | 景点名称 | 时间 |
| :--: | :--: | :------: | :--: |
|117.024489|36.669213|曲水亭街|20240611155103|
|117.016089|36.661138|趵突泉|20240611155103|
|116.813744|36.541549|济南国际园博园|20240611155103|
|117.022959|36.668068|芙蓉街|20240611155103|
|117.034920|36.641749|千佛山|20240611155103|
|117.023837|36.674997|大明湖|20240611155103|
|117.023837|36.674997|大明湖|20240611155103|
|117.024489|36.669213|曲水亭街|20240611155103|
|117.016089|36.661138|趵突泉|20240611155103|
|117.021483|36.661473|济南泉城广场|20240611155103|
|...|...|...|...|

#### 3.2 Kafka 消息队列
##### 3.2.1 Kafka 简介

点击访问：[Kafka官网](https://kafka.apache.org/)

***Apache Kafka*** 是一个开源分布式**事件(Event)**流平台，已被数千家公司用于高性能数据管道、流分析、数据集成和关键任务应用程序。

本项目主要使用**发布**（写入）和**订阅**（读取）事件流，包括从其他系统持续导入/导出数据；根据需要持久可靠地**存储**事件流；在事件发生时或回顾性地**处理**事件流。

##### 3.2.2 Kafka 中的核心概念

| 名词 | 解释 |
| :--- | :--- |
|`Broker`|***Kafka*** 集群包含一个或多个服务器，这些服务器称为 `Broker`|
|`Producer`|生产者，负责将数据发送到 ***Kafka***|
|`Consumer`|消费者，负责从 ***Kafka*** 中读取数据|
|`Consumer Group`|消费者组，多个消费者组成的组|
|`Topic`|主题，每条发布到 ***Kafka*** 集群的消息都有一个类别，这个类别称为 `Topic`，可以理解为文件夹|
|`Partition`|分区，每个`Topic`包含一个或多个`Partition`|

##### 3.2.3 Kafka 部署

- **Kafka** 的部署方式分为：
  - 分布式部署（多节点多Broker）
  - 单机部署（单节点单Broker/单节点多Broker）

- **Kafka** 是使用 **Scala** 编写的组件，依赖与Scala版本

- **Kafka** 依赖于 **ZooKeeper**，必须要安装 **ZooKeeper**，再安装 **Kafka**

- 部署 **Kafka** 过程

  - 解压 `kafka` 安装包
  - 配置环境变量
  - 修改配置 `config/server.properties` 文件

  ```properties
  # 配置 Broker 的 ID, 在同一个集群上, 这个值必须是一个独一无二的整数值
  broker.id=0
  
  # 配置日志文件的路径
  log.dirs=/home/subowen/apps/kafka_2.12-2.8.0/kafka-logs
  ```

##### 3.2.4 Kafka 的基本应用

- 启动 **ZooKeeper**

  ```bash
  [subowen@bigdata ~]$ zkServer.sh start
  ```

- 启动 `Kafka`

  - 第一次启动：先使用前台启动，如果没有问题再使用后台启动

  - 前台启动命令：`kafka-server-start.sh  $KAFKA_HOME/config/server.properties`

  - 后台启动命令：`kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties`

- 常用的 `Kafka` 命令

  参考博客：[Kafka 基础理论与常用命令详解 (超详细) Kafka常用命令和解释 - CSDN博客](https://blog.csdn.net/zcs2312852665/article/details/134979882)

  - `Topics` 常用命令

    - 创建一个名为`my_topic`的主题：

    ```bash
    [subowen@bigdata ~]$ kafka-topics.sh --create --bootstrap-server bigdata:9092 --topic my_topic
    ```

    - 查看名为`my_topic`的主题的详细信息：

    ```bash
    [subowen@bigdata ~]$ kafka-topics.sh --describe --bootstrap-server bigdata:9092 --topic my_topic
    ```

  - `Producer` 常用命令

    - 生产信息

    ```bash
    [subowen@bigdata ~]$ kafka-console-producer.sh --broker-list bigdata:9092 --topic my_topic
    ```

  - `Consumer` 常用命令


##### 3.2.5 使用 Flume 收集数据到 Kafka
在之前的项目中，我学习了 ***离线类型*** 项目的 Flume 数据收集，因为本次需要进行在线的实时数据分析，所以按照之前的离线分析方式，使用 Flume 将数据收集到 HDFS 是不适合实时的数据分析环境的。
将数据落地到 HDFS 则意味着数据进入磁盘，数据的读写会占用大量的磁盘 I/O，不适用于实时场景。
因此在实时项目，考虑到数据的实时性，本次实时数据分析项目使用 消息队列（Kafka）进行数据的存放。

1. 配置 `webserver01` 和 `webserver02` 上的 Flume 配置文件 `taildir-avro-stream.conf`
```conf
a1.sources  = r1
a1.sinks    = k1
a1.channels = c1

a1.sources.r1.type = TAILDIR
a1.sources.r1.positionFile = /home/subowen/apps/apache-flume-1.9.0-bin/position/taildir_position.json
a1.sources.r1.filegroups = f1
a1.sources.r1.filegroups.f1 = /home/subowen/serverJar/logweb/logs/scenic.log
a1.sources.r1.headers.f1.headerKey1 = inspur-szy
a1.sources.r1.fileHeader = true
a1.sources.r1.maxBatchCount = 1000

a1.sinks.k1.type = avro
a1.sinks.k1.hostname = 192.168.26.110
a1.sinks.k1.port = 4545

a1.channels.c1.type = memory
a1.channels.c1.capacity = 1000
a1.channels.c1.transactionCapacity = 100

a1.sources.r1.channels=c1
a1.sinks.k1.channel=c1
```

2. 配置 `bigdata` 上的 Flume 配置文件 `avro-kafka-stream.conf`
```conf
# 配置 Agent a1各个组件的名称
# Agent a1 的 source有一个, 叫做r1
a1.sources  = r1
# Agent a1 的 sink也有一个, 叫做k1
a1.sinks    = k1
# Agent a1 的 channel有一个, 叫做c1
a1.channels = c1

# 配置 Agent a1的source r1的属性
a1.sources.r1.type = avro
a1.sources.r1.bind = 0.0.0.0
# 监听的端口
a1.sources.r1.port = 4545

# 配置 Agent a1的sink k1的属性
a1.sinks.k1.type = org.apache.flume.sink.kafka.KafkaSink
a1.sinks.k1.kafka.topic = subowen
a1.sinks.k1.kafka.bootstrap.servers = bigdata:9092
a1.sinks.k1.kafka.flumeBatchSize = 20
a1.sinks.k1.kafka.producer.acks = 1
a1.sinks.k1.kafka.producer.linger.ms = 1
a1.sinks.k1.kafka.producer.compression.type = snappy

# 配置 Agent a1 的 channel c1 的属性, channel是用来缓冲Event数据的
# channel 的类型是内存 channel, 顾名思义这个 channel 是使用内存来缓冲数据
a1.channels.c1.type = memory
# 内存channel的容量大小是1000, 注意这个容量不是越大越好, 配置越大, 一旦Flume 挂掉丢失的 event 也就越多
a1.channels.c1.capacity = 1000
# source 和 sink 从内存 channel 每次事务传输的event数量
a1.channels.c1.transactionCapacity = 100

# 把source和sink绑定到channel上
# 与source r1绑定的channel有一个, 叫做c1
a1.sources.r1.channels = c1
# 与sink k1绑定的channel有一个, 叫做c1
a1.sinks.k1.channel = c1
```
3. 启动 `webserver01` 和 `webserver02` 的 Flume，这里为了简化操作，只启动一台机器上的 Flume
```bash
[subowen@webserver01 config]$ ./start-flume.sh taildir-avro-stream.conf a1
```
4. 启动 `bigdata` 上的 Flume
```bash
[subowen@bigdata config]$ ./start-flume.sh avro-kafka-stream.conf a1
```
5. 在 `bigdata` 上启动 **`kafka-console-consumer`**，使用消费者Shell进行测试消费，后期会更换为 ***`SparkStreaming`*** 进行消费。
```bash
[subowen@bigdata ~]$ kafka-console-consumer.sh --topic subowen --bootstrap-server bigdata:9092
```
6. 在 Windows 下运行客户端，模拟数据的产生，执行`ScenicAPP`代码，模拟数据的生成过程。

```java
package com.example.x.data;

import com.example.x.data.producer.DataProducer;

public class ScenicApp {
    public static final String url = "http://192.168.26.111:9527/logweb/upload";
    public static void main(String[] args) throws Exception{
        DataProducer.producer(url);
    }
}
```

### 📈 4. 数据实时分析

**Spark 3.0.1** 官方文档入口：[Overview - Spark 3.0.1 Documentation (apache.org)](https://spark.apache.org/docs/3.0.1/)

#### 4.1 `SparkStreaming` 概述

`SparkStreaming` 官方文档入口：[Spark Streaming - Spark 3.0.1 Documentation (apache.org)](https://spark.apache.org/docs/3.0.1/streaming-programming-guide.html)

`SparkStreaming` 类似于之前学习的 `SparkRDD` 和`SparkSQL`，是 **Spark API** 的核心扩展，支持实时数据流的可扩展、高吞吐量和容错。数据可以从`Kafka`、`Flume`、`Kinesis`或`TCP Socket` 等许多来源中读取，并且可以使用复杂的算法进行处理，这些算法用高级函数（如 `map`、`reduce`、`join` 和 `window`）表示。最后，处理过的数据可以保存到文件系统、数据库和实时仪表板。事实上，您可以在数据流上应用 **Spark** 的机器学习和图形处理算法。

<img src="https://img-blog.csdnimg.cn/direct/92d09a65f9c04aa09233f322ec8fd1b9.jpeg#pic_center" alt="SparkStream" style="zoom:67%;" />

`SparkStreaming` 是 `Spark` 中用于处理实时数据的一个模块。

在内部，他的工作流程是：`SparkStreaming` 接收实时输入的数据流，并对数据进行分批（微批）处理，由 **Spark** 引擎进行处理，生成最终的批量结果流。

<img src="https://img-blog.csdnimg.cn/direct/cf15f807ece04263a9a25833de244ee7.jpeg#pic_center" alt="SparkStreamFlow" style="zoom: 80%;" />

`SparkStreaming` 是微批处理（批次特别小，足以实现实时处理），不是真正的流处理。

这里也就可以更显著的得到离线批数据和实时数据之间的区别：

- **离线批数据：**bound——有界的数据

- **实时数据：**unbound——无界的数据

#### 4.2 开发第一个`SparkStreming`案例

类比 `SparkRDD` 开发的流程，我们给出`SparkStreaming` 开发的流程。

`SparkRDD` 编程模型：

1. 创建 `SparkContext`
2. 读取数据源
3. 处理数据
4. 输出结果
5. 关闭 `SparkContext`

`SparkStreaming` 编程模型：

1. 创建 `StreamingContext`
2. 读取数据
3. 处理数据
4. 输出结果
5. 启动程序（阻塞）
6. 等待程序关闭

由此，我们开发我们的第一个 `SparkStreaming` 实例。

1. 在 IDEA 中添加 Maven 依赖

```xml
<dependencies>
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-streaming_2.12</artifactId>
        <version>3.0.1</version>
    </dependency>
</dependencies>
```

2. 创建 `TcpStreamingAPP`，编写代码

```scala
package com.example.x.sparkstream

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
```

3. 在 `bigdata` 中安装和启动 `netcat` 生成数据

```bash
[subowen@bigdata ~]$ sudo yum -y install nc
[subowen@bigdata ~]$ nc –lk 9999
```

4. 启动程序进行测试，需要注意的是：程序只计算当前时间点发送过来的数据（无状态）。

#### 4.3 `SparkStreaming` 核心概念

| 概念                            | 解释                                                         |
| ------------------------------- | ------------------------------------------------------------ |
| `StreamingContext`              | `SparkStreaming`功能的主要入口点，它提供了用于从各种输入源创建 `DStream` 的方法。<br>创建和转换`DStream`后，可以分别使用 `start()` 和 `stop()` 启动和停止流计算。<br>`awaitTermination()` 等待执行停止，允许当前线程通过`stop()` 手动停止或通过一个异常等待`StreamingContext`的终止。 |
| `DStream`                       | `DStream` 是 `SparkStreaming` 提供的基本抽象，它表示一个连续的数据流，要么是从`Source`接收的输入数据流，要么是通过转换输入流生成的处理数据流。在内部，`DStream`由一系列连续的`RDD`表示，`DStream`中的每个`RDD`都包含一定时间间隔的数据。<br><img src="https://img-blog.csdnimg.cn/direct/0e07b9cf6c61446290e8e6cae12b3316.jpeg#pic_center" style="zoom:50%;" /><br>在`DStream`上应用的任何操作都转换为在底层 `RDD`上的操作。<br><img src="https://img-blog.csdnimg.cn/direct/45176000102c4e97bcfce70bf868d4f1.jpeg#pic_center" style="zoom:50%;" /><br>`DStream`中是由每个批次生成的`RDD`组成的。 |
| `Input DStreams` 和` Receivers` | `Input DStreams`是表示从数据源接收的输入数据流的`DStreams`。每个`Input DStream`（文件流除外）都与一个`Receiver`对象相关联，接收来自源的数据并将其存储在`Spark`的内存中进行处理。 |

#### 4.4 数据源

数据源其实就是从哪里读取数据，区别就在于一些读取的写法有不同。

##### 4.4.1 基本数据源

- `Socket`套接字数据源：`socketTextStream()`。

- `File System`文件系统数据源：`textFileStream()`。

##### 4.4.2  高级数据源

`Spark Kafka`官方文档：[Spark Streaming + Kafka Integration Guide (Kafka broker version 0.10.0 or higher) - Spark 3.0.1 Documentation (apache.org)](https://spark.apache.org/docs/3.0.1/streaming-kafka-0-10-integration.html)

- 高级数据源：如 `kafka` 、`Kinesis`等。
- 用代码对接 `Kafka`

```scala
package com.example.x.sparkstream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.codehaus.jackson.map.deser.std.StringDeserializer


object KafkaStreamApp {
    def main(args: Array[String]): Unit = {
        // 第一步
        // Spark 中遇到的一切序列化问题都需要 KryoSerializer
        val conf: SparkConf = new SparkConf().setMaster("Local[2]").setAppName("KafkaStreaming").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
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
        val topics = "subowen"
        val kafkaStream = KafkaUtils.createDirectStream[String, String](
            streamingContext,
            PreferConsistent,
            Subscribe[String, String](topics, kafkaParams)	// 指定订阅的 Topic
        )
        
        kafkaStream.flatMap(x=>x.value().split(",")).map((_, 1)).reduceByKey(_ + _).print()

        // 启动 StreamingContext
        streamingContext.start()

        // 阻塞 StreamingContext
        streamingContext.awaitTermination()
    }
}
```

##### 4.4.3 维护 Kafka Offset

在前面的学习中，我们已经了解了两种消费者的消费策略，分别是 `Lastest` 和 `Earliest`，这两种消费策略都是存在问题的。

- `Lastest` 消费策略：当消费者启动之后，从启动后产生的第一条数据开始消费
  - 消费者第一次启动之前，`topic`中已经存在的数据是不会被消费。
  - 消费者宕机的时间段内，`topic`中产生的数据不会被消费。

- `Earliest` 消费策略：



如何手动维护**Kafka Offset**？

我们可以将 **Offset** 持久化到一个数据库中，如`MySQL`、`HDFS`、`ZooKeeper` 中。下面将演示如何使用 `MySQL` 来维护 `Kafka Offset`。

1. 设计表结构

|   字段名    |   约束   |    类型     |         备注         |
| :---------: | :------: | :---------: | :------------------: |
|   k_topic   | 联合主键 | VARCHAR(50) | 可以直接从代码中获得 |
|  k_groupid  | 联合主键 | VARCHAR(50) | 可以直接从代码中获得 |
| k_partition | 联合主键 |     INT     |          -           |
|  k_offset   |    -     |   BIGINT    |          -           |

2. 实现表结构

```sql
CREATE TABLE IF NOT EXISTS t_offset (
    k_topic       VARCHAR(50)   NOT NULL,           -- 设置 Topic
    k_groupid     VARCHAR (50)  NOT NULL,           -- 设置 groupid
    k_partition   INT           NOT NULL,           -- 设置 Partition
    k_offset      BIGINT        NOT NULL,           -- 设置 offset
    PRIMARY KEY(k_topic, k_groupid, k_partition)    -- 设置联合主键
);
```

3. 开发工具类


```scala
package com.example.x.utils

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
```

4. 代码案例

```scala
package com.example.x.quickstart

import java.sql.{Connection, PreparedStatement}

import com.example.x.utils.{ConnectionUtils, KafkaOffsetManagerUtils}
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
```

5. 测试

#### 4.5 转换操作

#### 4.6 输出操作

将数据输出到指定的数据仓库中，如`MySQL`、`Redis`、`HBase`等。下面通过一个简单的 `WordCount` 案例快速了解如何将数据写入 `MySQL`。本案例借助

- 创建数据库和表

```sql
CREATE DATABASE travel CHARSET utf8;
CREATE TABLE wc(
    id 		BIGINT PRIMARY KEY AUTO_INCREMENT,
    word 	VARCHAR(50),
    count 	BIGINT
 );
```

- 存放 `Druid` 的配置文件

```properties
url             =   jdbc:mysql://bigdata:3306/travel?useSSL=false&characterEncoding=UTF-8
username        =   root
password        =   root
driverClassName =   com.mysql.jdbc.Driver
initialSize     =   5
maxActive       =   20
minIdle         =   1
maxWait         =   60000
validationQuery =   SELECT 1
testOnBorrow    =   true
testWhileIdle   =   true
```

- 编写数据库连接工具类`ConnectionUtils.scala`

```scala
package com.example.x.utils

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
```

- 编写 `WordCount` 代码

```scala
package com.example.x.quickstart

import java.sql.{Connection, PreparedStatement}

import com.example.x.utils.ConnectionUtils
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
```

#### 4.7 任务实施

1. 启动 `WebServer01` 的 `Flume`

```bash
[subowen@webserver01 config]$ ./start-flume.sh taildir-avro-stream.conf a1
```

2. 启动 `Bigdata` 的 `Flume`

```bash
[subowen@bigdata config]$ ./start-flume.sh taildir-avro-stream.conf a1
```

3. 启动 `WebServer01` 的生产者`Producer`，即`logweb-1.0.jar`

```bash
[subowen@webserver01 serverJar]$ ll
总用量 17608
drwxrwxr-x. 3 subowen subowen       18 6月  11 12:10 logweb
-rw-rw-r--. 1 subowen subowen 18014926 6月  11 12:08 logweb-1.0.jar
-rw-------. 1 subowen subowen     5531 6月  12 13:41 nohup.out
-rwxrw-r--. 1 subowen subowen       33 6月  11 12:09 start-logweb.sh
[subowen@webserver01 serverJar]$ cat ./start-logweb.sh
nohup java -jar logweb-1.0.jar &
[subowen@webserver01 serverJar]$ ./start-logweb.sh
```

4. 编写消费者程序，进行实时数据处理。

```scala
package com.example.x.sparkstream

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

        // 1. 统计相同经纬度和时间的人流量数据
        val peopleCountPerLocationAndTime: DStream[(String, Int)] = kafkaStream.map(_.value)
          .map(line => {
              val splitData = line.split(",")
              (s"${splitData(0)},${splitData(1)},${splitData(2)},${splitData(3)}", 1)
          })
          .reduceByKey(_ + _)

        // 2. 基于状态操作统计每个景点每分钟的人流量
        val peopleCountPerMinutePerLocation = peopleCountPerLocationAndTime.map(line => {
            val keySplit = line._1.split(",")
            (s"${keySplit(2)},${keySplit(3).substring(0, 12)}", line._2)
        }).reduceByKey(_ + _)

        // 输出结果
        peopleCountPerLocationAndTime.print()
        peopleCountPerMinutePerLocation.print()

        // 启动 StreamingContext
        streamingContext.start()

        // 阻塞 StreamingContext
        streamingContext.awaitTermination()
    }
}
```

5. 在 Windows 平台运行**生产者程序**`ScenicAPP`和**消费者程序**`RealtimeScenicPA`。结果如下：

<img src="https://img-blog.csdnimg.cn/direct/92f5069cf4fd4e859106d0c07633b718.jpeg#pic_center" style="zoom:50%;" />

<img src="https://img-blog.csdnimg.cn/direct/1f33579ba7e54ee185805243ab008ead.jpeg#pic_center" style="zoom:50%;" />

6. 结合 `MySQL` 进行数据持久化，首先建立好数据库和数据表。

```sql
-- SQL 建表语句如下
CREATE DATABASE IF NOT EXISTS travel CHARSET UTF8;

USE travel;

CREATE TABLE IF NOT EXISTS people_count_per_location_and_time (
    -- id           BIGINT PRIMARY KEY AUTO_INCREMENT,  -- id
    longitude       DOUBLE,                             -- 经度
    latitude        DOUBLE,                             -- 纬度
    scenic          VARCHAR(20),                        -- 景点
    sec_moment      VARCHAR(15),                        -- 具体时刻
    sec_quantity    BIGINT,                             -- 具体数量
    PRIMARY KEY(scenic, sec_moment)                     -- 设置联合主键
);

CREATE TABLE IF NOT EXISTS people_count_per_minute_per_location (
    -- id           BIGINT PRIMARY KEY AUTO_INCREMENT,  -- id
    scenic          VARCHAR(20),                        -- 景点
    min_moment      VARCHAR(15),                        -- 具体时刻
    min_quantity    BIGINT,                             -- 具体数量
    PRIMARY KEY(scenic, min_moment)                     -- 设置联合主键
);
```

7. 集合之前 `WordCount` 案例中编写 `ConnectionUtils.scala` 代码，完成该项目的数据持久化任务。编写`RealtimeScenicPA.scala`代码如下：

```scala
package com.example.x.travel

import java.sql.{Connection, PreparedStatement}

import com.example.x.utils.ConnectionUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object RealtimeScenicPA {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("RealtimeScenic").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.port.maxRetries", "100")
        val streamingContext = new StreamingContext(conf, Seconds(5))
        streamingContext.checkpoint("ckpt")

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
                      |INSERT INTO people_count_per_location_and_time(longitude, latitude, scenic, sec_moment, sec_quantity)
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
                      |INSERT INTO people_count_per_minute_per_location(scenic, min_moment, min_quantity)
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

        // peopleCountPerLocationAndTime.print()
         peopleCountPerMinutePerLocation.print()

        // 启动 StreamingContext
        streamingContext.start()

        // 阻塞 StreamingContext
        streamingContext.awaitTermination()
    }
}
```

8. 结合 `MySQL` 对 `t_offset` 表进行维护。

```scala
package com.example.x.travel

import java.sql.{Connection, PreparedStatement}

import com.example.x.utils.{ConnectionUtils, KafkaOffsetManagerUtils}
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
```

### 🎥 5. 数据实时监控系统

#### 5.1 软件开发体系架构

##### 5.1.1 开发架构

| 架构                     | 说明                                   |
| ------------------------ | -------------------------------------- |
| B/S架构（浏览器/服务器） | 只要用户安装浏览器（谷歌）就可以访问   |
| C/S架构（客户端/服务器） | 需要用户安装和更新客户端部分，比如微信 |

##### 5.1.2 开发模式及相关技术

- 开发模式：`MVC` 模式

| 组件       | 说明                                   |
| ---------- | -------------------------------------- |
| Model      | 模型                                   |
| View       | 视图，`login.html` / `index.html`      |
| Controller | 控制器，接受用户的请求，向用户做出响应 |

- 开发方式

| 方式         | 说明                       |
| ------------ | -------------------------- |
| 前后端不分离 | `JSP`/`HTML` + `SSM`       |
| 前后端分离   | 前端和后台不在同一个项目中 |

- 请求方法

| 请求方法  | 说明                 | 常用方式                                                     |
| --------- | -------------------- | ------------------------------------------------------------ |
| **`GET`** | 常用于查询、删除操作 | 1. 浏览器的地址栏<br />2. `<a href="..."></a>`<br />3. `Ajax`<br />4. 异步请求 |
| `POST`    | 常用于增加、修改操作 | 1.  `Form`表单<br />2. `Ajax`                                |

- `JavaWeb`开发相关技术
  - Java后台技术
    - `SSM`：大量的配置文件 `xml`， `SpringBoot`+ `MyBatis`
    - `Servlet`/`JSP`、`Spring` + `Spring MVC`、`SpringBoot`、`SpringCloud`（微服务） 
    - 数据库相关框架：`MyBatis`、`Hibernate`、`Spring JPA`

  - 前端技术
    - `HTML/HTML5`、`CSS/CSS3`、`JS`
    - 前端框架：`jQuery`、`Vue`、`AngularJS`、`React`、`TS`


#### 5.2 `SpringBoot` + `MyBatis` 框架简介

[Spring Boot：入门篇 (cnblogs.com)](https://www.cnblogs.com/ityouknow/p/5662753.html)

[MyBatis中文网](https://mybatis.net.cn/)

#### 5.3 后台服务开发

基本任务说明：

- 统计今天所有景点的人流量总和
- 统计当前时间（分钟）所有景点的人流量总和
- 统计每分钟每个景点的人流量（经纬度）-热力图

##### 5.3.1 项目环境搭建

1. 创建`SpringBoot`项目

<img src="./OnlineTravelBigdataPlatform/NewModule.png" style="zoom: 50%;" >

<img src="./OnlineTravelBigdataPlatform/NewModule2.png" style="zoom: 50%;" >

<img src="./OnlineTravelBigdataPlatform/NewModule3.png" style="zoom: 50%;" >

<img src="./OnlineTravelBigdataPlatform/NewModule4.png" style="zoom: 50%;" >

2. 修改必要配置

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <!-- 这里需要修改版本 -->
        <version>2.7.15</version>
        <relativePath/> <!-- lookup parent from repository -->
    </parent>
    <groupId>com.example.x.travel</groupId>
    <artifactId>travelweb</artifactId>
    <version>0.0.1-SNAPSHOT</version>
    <name>travelweb</name>
    <description>Travel Project Web</description>
    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>

        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <optional>true</optional>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>mysql</groupId>
            <artifactId>mysql-connector-java</artifactId>
            <version>5.1.41</version>
        </dependency>
        <dependency>
            <groupId>com.alibaba</groupId>
            <artifactId>druid-spring-boot-starter</artifactId>
            <version>1.2.18</version>
        </dependency>
        <dependency>
            <groupId>org.mybatis.spring.boot</groupId>
            <artifactId>mybatis-spring-boot-starter</artifactId>
            <version>1.3.2</version>
        </dependency>
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
        </dependency>
        <dependency>
            <groupId>org.apache.commons</groupId>
            <artifactId>commons-lang3</artifactId>
        </dependency>
        <dependency>
            <groupId>log4j</groupId>
            <artifactId>log4j</artifactId>
            <version>1.2.17</version>
        </dependency>

    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
                <configuration>
                    <excludes>
                        <exclude>
                            <groupId>org.projectlombok</groupId>
                            <artifactId>lombok</artifactId>
                        </exclude>
                    </excludes>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

3. 修改 `application.xml` 为 `application.yml` 并进行配置

```yaml
# 配置应用服务器端口以及根目录
server:
  port: 9090
  servlet:
    context-path: /travelserver
# 配置应用名
spring:
  application:
    name: travelserver
  # 配置数据源, 这里是 Druid 的一些配置
  datasource:
    type: com.alibaba.druid.pool.DruidDataSource
    druid:
      url: jdbc:mysql://bigdata:3306/travel?useSSL=false&characterEncoding=UTF-8
      username: root
      password: root
      driver-class-name: com.mysql.jdbc.Driver
      initial-size: 1
      minIdle: 1
      maxActive: 5
      maxWait: 6000
      timeBetweenEvictionRunsMillis: 6000
      minEvictableIdleTimeMillis: 30000
      validationQuery: SELECT 1 FROM DUAL
      testWhileIdle: true
      testOnBorrow: false
      testOnReturn: false
      filters: stat,wall,log4j
# 配置 MyBatis 的路径
mybatis:
  # 这里是 DAO Mapper的路径, 表示 Resource/Mapper/ 的所有 *.xml 文件
  mapper-locations: classpath:mapper/*.xml
  # 这里定义实体类的位置
  type-aliases-package: com.example.x.travel.travelweb.entity
  configuration:
    map-underscore-to-camel-case: true
```

4. 在 **IDEA** 中添加 `Lombok` 和 `MyBatisX` 的插件
5. 配置 `MyBatis` 的 `*.xml` 模板

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE mapper PUBLIC "-//mybatis.org//DTD Mapper 3.0//EN"
      "http://mybatis.org/dtd/mybatis-3-mapper.dtd">
<mapper> <!-- 这里补全内容 -->

</mapper>
```

6. 开发 `TravelConfig.java` 类解决跨域问题

```java
// SpringBoot 中很多事情需要通过注解的方式来实现
// @... 代表注解
// @Configuration 注解表明这是一个 Spring Boot 的配置类
// @EnableWebMvc  开启 WebMvc 的功能, 这样 Spring Boot 就能自动处理 Web 相关的配置
@Configuration
@EnableWebMvc
public class TravelConfig implements WebMvcConfigurer {
    /**
     * @brief 设置服务器跨域访问策略: 所有 GET 和 POST 请求都可以跨域访问, 允许所有来源的请求, 即允许跨域请求
     *        因为我们的项目是前后端分离的, 即前端和后端不在一个服务器上, 所以需要处理跨域问题
     * @param registry
     */
    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**")  // addMapping 方法的参数代表需要进行 CORS 映射的 HTTP方法及其路径, 这里是"/**", 意味着对所有HTTP方法进行映射
                .allowedOriginPatterns("*")     // allowedOriginPatterns 是一个正则表达式列表, 这里使用 "*" 表示任何来源都可以
                .allowedMethods("GET", "POST")  // allowedMethods是允许的 HTTP 方法，这里是 "GET,POST"
                .allowCredentials(true)         // 允许携带请求体的请求通过, 这里的 allowCredentials(true) 意味着在请求头中携带用户凭证信息是允许的
                .maxAge(3600);                  // maxAge 设置 CORS 跨域配置的缓存时间, 这里是 3600 秒
    }
}
```

7. 本项目（`Java`开发）中层与层之间的联系

<img src="./OnlineTravelBigdataPlatform/LayersToExecuteTravelWeb.png" style="zoom: 33%;" />

##### 5.3.2 后台开发

- `com.example.x.travel.travelweb.config.TravelConfig`

```java
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

// SpringBoot 中很多事情需要通过注解的方式来实现
// @... 代表注解
// @Configuration 注解表明这是一个 Spring Boot 的配置类
// @EnableWebMvc  开启 WebMvc 的功能, 这样 Spring Boot 就能自动处理 Web 相关的配置
@Configuration
@EnableWebMvc
public class TravelConfig implements WebMvcConfigurer {
    /**
     * @brief 设置服务器跨域访问策略: 所有 GET 和 POST 请求都可以跨域访问, 允许所有来源的请求, 即允许跨域请求
     *        因为我们的项目是前后端分离的, 即前端和后端不在一个服务器上, 所以需要处理跨域问题
     * @param registry
     */
    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**")  // addMapping 方法的参数代表需要进行 CORS 映射的 HTTP方法及其路径, 这里是"/**", 意味着对所有HTTP方法进行映射
                .allowedOriginPatterns("*")     // allowedOriginPatterns 是一个正则表达式列表, 这里使用 "*" 表示任何来源都可以
                .allowedMethods("GET", "POST")  // allowedMethods是允许的 HTTP 方法，这里是 "GET,POST"
                .allowCredentials(true)         // 允许携带请求体的请求通过, 这里的 allowCredentials(true) 意味着在请求头中携带用户凭证信息是允许的
                .maxAge(3600);                  // maxAge 设置 CORS 跨域配置的缓存时间, 这里是 3600 秒
    }
}
```

- `com.example.x.travel.travelweb.controller.TravelController`

```java
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController                 // 注解: 定义这个类是一个 Controller
@RequestMapping("/scenic")      // 注解: 定义访问这个类的 URL
// 访问 URL: http://ip:port/scenic
public class TravelController {
    @Autowired
    private TravelService travelService;

    // @RequestMapping 表示既可以通过 GET 访问, 也可以通过 POST 访问
    // @GetMapping     表示只可以通过 GET 访问
    // @PostMapping    表示只可以通过 POST 访问
    /**
     * @brief   表示获取一天访客的总数量
     * @url     http://localhost:9090/travelserver/scenic/getDaySum
     * @return
     */
    @RequestMapping("/getDaySum")
    public long getDaySum() {
        long sum = travelService.getDaySum();
        return sum;
    }

    @RequestMapping("/getCurrentSum")
    public long getCurrentSum() {
        long sum = travelService.getCurrentSum();
        return sum;
    }

    @RequestMapping("/getHeatData")
    public List<Scenic> getHeatData() {
        return travelService.getHeatData();
    }

    @RequestMapping("/getScenicMinuteData")
    public List<Scenic> getScenicMinuteData() {
        return travelService.getScenicMinuteData();
    }

    @RequestMapping("/getScenicTrend")
    public HashMap<String, Object> getScenicTrend() {
        return travelService.getScenicTread();
    }

    @PostMapping("/selectForm")
    @ResponseBody
    public Object receiveFormData(@RequestBody Map<String, String> formData) {
        // 在这里可以处理收到的formData
        // System.out.println(formData);
        String selectDate = formData.get("date");
        String selectFromTime = formData.get("fromTime");
        String selectToTime = formData.get("toTime");
        String selectTableName = formData.get("tableName");
        // System.out.println(selectDate + " " + selectFromTime + " " + selectToTime + " " + selectTableName);

        // 执行业务逻辑
        if (selectDate.equals("")) {
            return null;
        }
        String fromDateAndTime = selectDate.replace("-", "") + selectFromTime.replace(":", "").replace("：", "");
        String toDateAndTime = selectDate.replace("-", "") + selectToTime.replace(":", "").replace("：", "");
        System.out.println("From Date And Time: " + fromDateAndTime + "\nTo Date And Time: " + toDateAndTime);
        switch (selectTableName) {
            case "0": {
                System.out.println("请进行选择");
                break;
            }
            case "1": { // 热力图
                return travelService.getHeatDataByDateAndTime(fromDateAndTime, toDateAndTime);
            }
            case "2": { // 人流量柱状图
                return travelService.getScenicDataByDateAndTime(fromDateAndTime, toDateAndTime);
            }
            case "3": { // 人流量趋势图
                return travelService.getScenicTreadByDateAndTime(fromDateAndTime, toDateAndTime);
            }
            default:    // 未定义图表
                break;
        }
        return "Data received successfully";
    }
}
```

- `com.example.x.travel.travelweb.dao.TravelDao`

```java
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.List;

@Repository
public interface TravelDao {
    long getDaySum();           // 查询当天时间内的所有数据之和
    long getCurrentSum();       // 查询当前时间内的所有数据之和
    List<Scenic> getHeatData();         // 查询用于创建热力图的数据
    List<Scenic> getScenicMinuteData(); // 查询用于获取每分钟景点人流量
    List<Scenic> getScenicTread();      // 查询用于获取每分钟景点人流量趋势

    List<Scenic> getHeatDataByDateAndTime(String fromDateAndTime, String toDateAndTime);
    List<Scenic> getScenicDataByDateAndTime(String fromDateAndTime, String toDateAndTime);
    List<Scenic> getScenicTreadByDateAndTime(String fromDateAndTime, String toDateAndTime);
}

```

- `com.example.x.travel.travelweb.entity.LineData`

```java
import lombok.Data;

import java.util.List;

@Data
public class LineData {
    private String      name;
    private List<Long>  data;
}
```

- `com.example.x.travel.travelweb.entity.Scenic`

```java
import lombok.Data;

@Data
public class Scenic {
    private double lng;
    private double lat;
    private long count;
    private String time;
    private String scenic;
}
```

- `com.example.x.travel.travelweb.services.TravelService`

```java
package com.example.x.travel.travelweb.services;

import com.example.x.travel.travelweb.entity.Scenic;

import java.util.HashMap;
import java.util.List;

// 接口: 定义规范的
public interface TravelService {
    long getDaySum();
    long getCurrentSum();
    List<Scenic> getHeatData();
    List<Scenic> getScenicMinuteData();
    HashMap<String, Object> getScenicTread();

    List<Scenic> getHeatDataByDateAndTime(String fromDateAndTime, String toDateAndTime);
    List<Scenic> getScenicDataByDateAndTime(String fromDateAndTime, String toDateAndTime);
    HashMap<String, Object> getScenicTreadByDateAndTime(String fromDateAndTime, String toDateAndTime);
}

```

- `com.example.x.travel.travelweb.services.impl.TravelServiceImpl`

```java
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Service
public class TravelServiceImpl implements TravelService {
    // 调用某个对象的方法, 需要先创建对象
    @Autowired
    private TravelDao travelDao;    // 相当于 TravelDap travelDao = new TravelDao();

    @Override
    public long getDaySum() {
        return travelDao.getDaySum() ;
    }

    @Override
    public long getCurrentSum() {
        return travelDao.getCurrentSum();
    }

    @Override
    public List<Scenic> getHeatData() {
        return travelDao.getHeatData();
    }

    @Override
    public List<Scenic> getScenicMinuteData() {
        return travelDao.getScenicMinuteData();
    }

    @Override
    public HashMap<String, Object> getScenicTread() {
        HashMap<String, Object> maps = new HashMap<>();
        List<String> times = new ArrayList<>();     // 时间数组
        List<LineData> series = new ArrayList<>();  // Series 数组

        List<Scenic> heat = travelDao.getScenicTread();     // 调用 DAO 方法查询数据
        HashMap<String, List<Long>> map = new HashMap<>();  // 将景点以及这个景点对应的所有数据整合成一条数据
        for(Scenic scenic: heat) {
            String time = scenic.getTime();                 // 获取数据库每条数据的时间
            if (!times.contains(time)) {                    // 这个时间在 times 中是否存在
                times.add(time);                            // 不存在则添加
            }
            String scenic_name = scenic.getScenic();        // 获取景点名称
            // 判断在 Map 中是否有 key, 并判断:
            // 如果有这个 key, 则把数据添加到 value 中
            // 如果没有这个 key, 则添加这个 key, 新添加一个集合
            map.computeIfAbsent(scenic_name, k->new ArrayList<>()).add(scenic.getCount());

            // 上面一行等同于下面的逻辑
            // if (map.containsKey(scienc)){
            //     map.get(scienc).add(scenic.getCount());
            // } else {
            //     List list=new ArrayList<Long>();
            //     list.add(scenic.getCount());
            //     map.put(scienc,list);
            // }
        }

        // System.out.println(times);
        // System.out.println(map);

        map.forEach((key, value) -> {
            LineData data = new LineData();
            data.setName(key);
            data.setData(value);
            series.add(data);
        });

        maps.put("time",    times);
        maps.put("series", series);
        return maps;
    }

    @Override
    public List<Scenic> getHeatDataByDateAndTime(String fromDateAndTime, String toDateAndTime) {
        return travelDao.getHeatDataByDateAndTime(fromDateAndTime, toDateAndTime);
    }

    @Override
    public List<Scenic> getScenicDataByDateAndTime(String fromDateAndTime, String toDateAndTime) {
        return travelDao.getScenicDataByDateAndTime(fromDateAndTime, toDateAndTime);
    }

    @Override
    public HashMap<String, Object> getScenicTreadByDateAndTime(String fromDateAndTime, String toDateAndTime) {
        return null;
    }

}

```

- `mapper/TravelMapper.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE mapper PUBLIC "-//mybatis.org//DTD Mapper 3.0//EN"
        "http://mybatis.org/dtd/mybatis-3-mapper.dtd">
<mapper namespace="com.inspur.szy.subowen.travel.travelweb.dao.TravelDao">


    <select id="getDaySum" resultType="java.lang.Long">
        SELECT IFNULL(SUM(min_quantity), 0) AS cnt FROM t_scenic WHERE SUBSTRING(`min_moment`, 1, 8)=DATE_FORMAT(NOW(), "%Y%m%d")
    </select>
    <select id="getCurrentSum" resultType="java.lang.Long">
        SELECT IFNULL(SUM(min_quantity), 0) AS cnt FROM t_scenic WHERE SUBSTRING(`min_moment`, 1, 12)=DATE_FORMAT(NOW(), "%Y%m%d%H%i")
    </select>
    <select id="getHeatData" resultType="com.inspur.szy.subowen.travel.travelweb.entity.Scenic">
        SELECT longitude AS `lng`, latitude AS `lat`, SUBSTRING(`sec_moment`, 1, 12) AS today, SUM(`sec_quantity`) AS `count`
        FROM t_heat GROUP BY lng, lat, today HAVING today=DATE_FORMAT(NOW(), '%Y%m%d%H%i')
    </select>
    <select id="getScenicMinuteData" resultType="com.inspur.szy.subowen.travel.travelweb.entity.Scenic">
        SELECT scenic, min_moment AS `time`, min_quantity AS `count`
        FROM t_scenic
        WHERE min_moment = (SELECT MAX(min_moment) FROM t_scenic WHERE <![CDATA[min_moment <= NOW()]]>)
        GROUP BY scenic, min_moment;
    </select>
    <select id="getScenicTread" resultType="com.inspur.szy.subowen.travel.travelweb.entity.Scenic">
        SELECT scenic, min_moment AS `time`, min_quantity AS `count`
        FROM t_scenic
        WHERE SUBSTRING(`min_moment`, 1, 8)=DATE_FORMAT(NOW(), '%Y%m%d') ORDER BY `time`
    </select>
    <select id="getHeatDataByDateAndTime" resultType="com.inspur.szy.subowen.travel.travelweb.entity.Scenic">
        SELECT longitude AS `lng`, latitude AS `lat`, SUBSTRING(`sec_moment`, 1, 12) AS day, SUM(`sec_quantity`) AS `count`
        FROM t_heat GROUP BY lng, lat, day HAVING <![CDATA[day >= #{fromDateAndTime} AND day <= #{toDateAndTime}]]>
    </select>
    <select id="getScenicDataByDateAndTime" resultType="com.inspur.szy.subowen.travel.travelweb.entity.Scenic">
        SELECT scenic, SUBSTRING(`min_moment`, 1, 12) AS `time`, min_quantity AS `count`
        FROM t_scenic
        WHERE <![CDATA[min_moment >= #{fromDateAndTime} AND min_moment <= #{toDateAndTime}]]>
        GROUP BY `scenic`, `min_moment`;
    </select>
    <select id="getScenicTreadByDateAndTime" resultType="com.inspur.szy.subowen.travel.travelweb.entity.Scenic">

    </select>
</mapper>
```

#### 5.4 前端开发

所有代码在 `GitHub` 均有开源，我做的也就只是添加 `Ajax` 请求而已。

### ❓ 问题汇总

| 序号 | 发生日期   | 问题描述                                                     | 是否解决 | 解决措施                                                     | 备注         |
| ---- | ---------- | ------------------------------------------------------------ | -------- | ------------------------------------------------------------ | ------------ |
| 0001 | 2024-06-13 | 执行代码：<br><img src="https://img-blog.csdnimg.cn/direct/2e01c35c37f143edaeef11a431df32d7.png#pic_center" alt="Qusetion0001" style="zoom: 50%;" /><br>出现编译错误：<br><img src="https://img-blog.csdnimg.cn/direct/499491d731204bc0a433dd4ab71fd5c2.png" alt="Qusetion0001" style="zoom: 50%;" /> | 是       | 修改代码：<br>`val topics = "..."` 为 `val topics = Array("...")`<br>这里报错的原因是出现了无法重载方法的问题。因为错误的传递了所需要的参数，导致`Subscribe`方法出现了不期待的重载。修改代码到正确的格式即可。 | 老师协助解决 |
| 0002 | 2024-06-13 | 使用`updateStatusByKey`出现数据库数据不断增加的问题<br>***问题原因***：<br>1. MySQL数据表设计不合理，设置了一个列为 `id` 并将其设置为主键<br>2. SQL语句有待优化 | 是       | 1. 修改MySQL表的结构，设置联合主键 <br>2. 修改SQL语句，使用 `ON DUPLICATE KEY` 语法 | 老师协助解决 |
| 0003 | 2024-06-13 | 执行程序出现错误：<br>`ERROR [main] (Logging.scala:94) - Failed to bind SparkUI`<br>***问题原因***：<br>每一个`Spark`任务都会占用一个`SparkUI`端口，默认为`4040`，如果被占用则依次递增端口重试。但是有个默认重试次数，为`16`次。`16`次重试都失败后，会放弃该任务的运行。 | 是       | 初始化 `SparkConf` 时，添加`conf.set("spark.port.maxRetries", "100")`语句；使用 `spark-submit`提交任务时，在启动命令行中添加`–conf spark.port.maxRetries=100 \`<br>该参数设置向后递增`100`次寻找端口 | 自主解决     |
| 0004 | 2024-06-17 | 不是很严重的问题，当`return 0` 或未产生数据时，使用 **Edge** 浏览器会出现屏幕上没有输出值的情况 | 是       | 替换为**Chrome**即可解决问题，不过也没啥必要                 | 水           |
| 0005 | 2024-06-18 | 在 `TravelMapper.xml` 中编写带 `<` 和`>` 的 `SQL` 代码，导致出现问题：<br />`Caused by: org.xml.sax.SAXParseException: 元素内容必须由格式正确的字符数据或标记组成。`<br />**问题原因**：<br />XML文件会将`<` 或 `>` 当作是标记，导致错误 | 是       | [参考文章](https://blog.csdn.net/miantian180/article/details/82255910)<br />使用标记`<![CDATA[ ]]>`将包含 `<` 或 `>` 的语句包含住，如：`<![CDATA[min_moment <= NOW()]]>` | 自主解决     |
| 0006 | 2024-06-19 | 同学出现问题：<br />生产者数据正常写入、客户端（Windows）和服务器端（Linux）时间正确，MySQL无法通过`NOW()` 获取到当前时间的数据。 | 是       | MySQL时区问题。                                              | 协助同学解决 |
| 0007 | 2024-06-20 | 同学出现问题：<br />虚拟机掉网卡                             | 是       | [虚拟机网卡不见了,重新开机虚拟机网卡消失连接不上--虚拟机网卡掉了](https://blog.csdn.net/ganchangshao/article/details/82992503) | 协助同学解决 |





