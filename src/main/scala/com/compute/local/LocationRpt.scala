package com.compute.local

import com.util.RptUtil
import org.apache.spark.sql.{DataFrame, SparkSession}

object LocationRpt {
    def main(args: Array[String]): Unit = {
        //设定目录限制
        if (args.length != 1) {
            println("目录不正确，退出程序")
            sys.exit()
        }

        val inputPath = args(0)

        val sparkSession: SparkSession = SparkSession
            .builder()
            .appName(this.getClass.getName)
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.debug.maxToStringFields", "100")
            .master("local[*]")
            .getOrCreate()
        val df: DataFrame = sparkSession.read.parquet(inputPath)

        df.rdd.map(row => {
            //根据指标的字段获取数据
            //REQUESTMODE	PROCESSNODE	ISEFFECTIVE	ISBILLING	ISBID	ISWIN	ADORDEERID
            val requestmode: Int = row.getAs[Int]("requestmode")
            val processnode: Int = row.getAs[Int]("processnode")
            val iseffective: Int = row.getAs[Int]("iseffective")
            val isbilling: Int = row.getAs[Int]("isbilling")
            val isbid: Int = row.getAs[Int]("isbid")
            val iswin: Int = row.getAs[Int]("iswin")
            val adorderid: Int = row.getAs[Int]("adorderid")
            val winprice: Double = row.getAs[Double]("winprice")
            val adpayment: Double = row.getAs[Double]("adpayment")

            val provincename: String = row.getAs[String]("provincename")
            val cityname: String = row.getAs[String]("cityname")
            //处理请求数
            val rptList: List[Double] = RptUtil.reqPt(requestmode, processnode)
            //处理展示点击
            val clickList: List[Double] = RptUtil.clickPt(requestmode, iseffective)
            //处理广告
            val adList: List[Double] = RptUtil.adPt(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)

             val all: List[Double] = rptList ++ clickList ++ adList

            ((provincename,cityname),all)
        }).reduceByKey((l1,l2)=>{
            //((1,1),(1,1))
            l1.zip(l2).map(t=>t._1+t._2)
        }).saveAsTextFile("abcd")
    }

}
