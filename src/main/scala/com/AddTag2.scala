package com

import com.label._
import com.typesafe.config.{Config, ConfigFactory}
import com.util.{HbaseUtil, TagUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object AddTag {
    def main(args: Array[String]): Unit = {
        //设定目录限制
        if (args.length != 2) {
            println("目录不正确，退出程序")
            sys.exit()
        }

        val Array(inputPath, day) = args
        val sparkSession: SparkSession = SparkSession
            .builder()
            .appName(this.getClass.getName)
            .master("local[*]")
            //设置序列化级别&压缩方式
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .getOrCreate()

        val config: Config = ConfigFactory.load()
        val tableName: String = config.getString("hbase.tablename")

        val configuration: Configuration = sparkSession.sparkContext.hadoopConfiguration

        val connection: Connection = HbaseUtil.getConnection(configuration)
        HbaseUtil.createTable(connection, tableName, "tags")

        val jobConf = new JobConf(configuration)
        // 指定输出类型
        jobConf.setOutputFormat(classOf[TableOutputFormat])
        // 指定输出哪张表
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

        val df: DataFrame = sparkSession.read.parquet(inputPath)
        val allUserId = df.rdd.map(row => {
            val idList: List[String] = TagUtils.getAllUserId(row)
            (idList, row)
        })
        val verties: RDD[(Long, List[(String, Int)])] = allUserId.flatMap(tuple2 => {
            val row: Row = tuple2._2
            val adLocation: List[(String, Int)] = AdLocationLabel.tag(row)
            val app: List[(String, Int)] = AppLabel.tag(row)
            val area: List[(String, Int)] = AreaLabel.tag(row)
            val channel: List[(String, Int)] = ChannelLabel.tag(row)
            val device: List[(String, Int)] = DeviceLabel.tag(row)
            val keywords: List[(String, Int)] = KeyWordLabel.tag(row)

            val business: List[(String, Int)] = BusinessLabel.tag(row)

            val list: List[(String, Int)] = adLocation ++ app ++ area ++ channel ++ device ++ keywords ++ business

            val VD = tuple2._1.map((_, 0)) ++ list

            tuple2._1.map(uId => {
                if (tuple2._1.head.equals(uId)) {
                    (uId.hashCode.toLong, VD)
                } else {
                    (uId.hashCode.toLong, List.empty)
                }
            })
        })
        //边集合
        val edges = allUserId.flatMap(tuple2 => {
            tuple2._1.map(uId => Edge(tuple2._1.head.hashCode.toLong, uId.hashCode.toLong, 0))
        })
        //构建图
        val graph = Graph(verties, edges)

        val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices

        //集合标签
        vertices.join(verties).map {

            case (uId, (cnId, tagsAndUserId)) => {
                (uId, tagsAndUserId)
            }
        }.reduceByKey((list1, list2) => {
            (list1 ++ list2).groupBy(_._1)
                .mapValues(_.map(_._2).sum)
                .toList
        }).map {
            case (userId, userTags) => {
                val put = new Put(Bytes.toBytes(userId))
                put.addImmutable(Bytes.toBytes("tags"), Bytes.toBytes(day), Bytes.toBytes(userTags.mkString(",")))
                (new ImmutableBytesWritable(), put)
            }
        }.saveAsHadoopDataset(jobConf)

        sparkSession.stop()
    }
}
