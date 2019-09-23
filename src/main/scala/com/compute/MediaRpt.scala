package com.compute

import com.util.RptUtil
import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

object MediaRpt {
    def main(args: Array[String]): Unit = {
        if (args.length != 3) {
            println("参数错误，退出")
            sys.exit()
        }

        val Array(inputPath, outputPath, dictPath) = args

        val sparkSession: SparkSession = SparkSession
            .builder()
            .appName(this.getClass.getName)
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.debug.maxToStringFields", "100")
            .master("local[*]")
            .getOrCreate()

        val df: DataFrame = sparkSession.read.parquet(inputPath)

        val map: collection.Map[String, String] = sparkSession.sparkContext.textFile(dictPath)
            .map(_.split("\\s", -1)).filter(_.length >= 5)
            .map(arr => (arr(4), arr(1))).collectAsMap()
        //广播
        val broadcastMap: Broadcast[collection.Map[String, String]] = sparkSession.sparkContext.broadcast(map)

        df.rdd.map(row => {
            val appName = row.getAs[String]("appname")
            if (StringUtils.isBlank(appName)) {
                broadcastMap.value.getOrElse(row.getAs[String]("appid"), "unknown")
            }
            val requestmode: Int = row.getAs[Int]("requestmode")
            val processnode: Int = row.getAs[Int]("processnode")
            val iseffective: Int = row.getAs[Int]("iseffective")
            val isbilling: Int = row.getAs[Int]("isbilling")
            val isbid: Int = row.getAs[Int]("isbid")
            val iswin: Int = row.getAs[Int]("iswin")
            val adorderid: Int = row.getAs[Int]("adorderid")
            val winprice: Double = row.getAs[Double]("winprice")
            val adpayment: Double = row.getAs[Double]("adpayment")

            val reqList: List[Double] = RptUtil.reqPt(requestmode, processnode)
            val clickList: List[Double] = RptUtil.clickPt(requestmode, iseffective)
            val adList: List[Double] = RptUtil.adPt(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)
            val all = reqList ++ clickList ++ adList

            (appName, all)
        }).reduceByKey((l1, l2) => {
            l1.zip(l2)
                .map(t => t._1 + t._2)
        }).map(t => t._1 + "," + t._2.mkString(","))
            .saveAsTextFile(outputPath)

        sparkSession.stop()
    }
}
