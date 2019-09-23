package com

import com.label._
import com.typesafe.config.{Config, ConfigFactory}
import com.util.{HbaseUtil, TagUtils}
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

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

        val df: DataFrame = sparkSession.read.parquet(inputPath)

        import sparkSession.implicits._
        df.filter(row => {
            val userId = TagUtils.getOneUserId(row)
            StringUtils.isNotBlank(userId)
        })
            .map(row => {
                val userId = row.getAs[String]("userid")
                val adLocation: List[(String, Int)] = AdLocationLabel.tag(row)
                val app: List[(String, Int)] = AppLabel.tag(row)
                val area: List[(String, Int)] = AreaLabel.tag(row)
                val channel: List[(String, Int)] = ChannelLabel.tag(row)
                val device: List[(String, Int)] = DeviceLabel.tag(row)
                val keywords: List[(String, Int)] = KeyWordLabel.tag(row)

                val business: mutable.HashMap[String, Int] = BusinessLabel.tag(row)

                val list: List[(String, Int)] = adLocation ++ app ++ area ++ channel ++ device ++ keywords ++ business
                (userId, list)
            }).rdd.reduceByKey((list1, list2) => {
            (list1 ::: list2)
                .groupBy(_._1)
                .mapValues(x => x.foldLeft[Int](0)(_ + _._2))
                .toList
        }).map {
            case (userId, userTags) => {
                val put = new Put(Bytes.toBytes(userId))
                put.addImmutable(Bytes.toBytes("tags"), Bytes.toBytes(day), Bytes.toBytes(userTags.mkString(",")))
                (new ImmutableBytesWritable(),put)
            }
        }.saveAsHadoopDataset(jobConf)

        sparkSession.close()
    }
}
