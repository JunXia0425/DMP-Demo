package com.compute

import com.label._
import com.util.Util
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

object AddTag {
    def main(args: Array[String]): Unit = {
        //设定目录限制
        if (args.length != 2) {
            println("目录不正确，退出程序")
            sys.exit()
        }

        val Array(inputPath, outPath) = args
        val sparkSession: SparkSession = SparkSession
            .builder()
            .appName(this.getClass.getName)
            .master("local[*]")
            //设置序列化级别&压缩方式
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .getOrCreate()

        val df: DataFrame = sparkSession.read.parquet(inputPath)

        import sparkSession.implicits._
        df
//            .filter(row => {
//            val userId = row.getAs[String]("userid")
//            StringUtils.isNotBlank(userId)
//        })
            .map(row => {
                val userId = row.getAs[String]("userid")
                val adLocation: mutable.HashMap[String, Int] = AdLocationLabel.tag(row)
                val app: mutable.HashMap[String, Int] = AppLabel.tag(row)
                val area: mutable.HashMap[String, Int] = AreaLabel.tag(row)
                val channel: mutable.HashMap[String, Int] = ChannelLabel.tag(row)
                val device: mutable.HashMap[String, Int] = DeviceLabel.tag(row)
                val keywords: mutable.HashMap[String, Int] = KeyWordLabel.tag(row)

                val business: mutable.HashMap[String, Int] = BusinessLabel.tag(row)
                val map: mutable.HashMap[String, Int] = Util.flatHashMap(adLocation, app, area, channel, device,
                    keywords, business)

                //                (userId, map)
                userId + "----" + map.mkString("|")
            }).write.text(outPath)

        //            .groupByKey(_._1)
        //            .mapValues(t => {
        //                //取出hashmap
        //                val map: mutable.HashMap[String, Int] = t._2
        //                map.reduce((x, y) => {
        //                    if (x._1.equals(y._1)) {
        //                        x._2 + y._2
        //                    }
        //                })
        //            })


        sparkSession.stop()
    }
}
