package com.compute

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object AreaDistrbute {
    def main(args: Array[String]): Unit = {
        val sparkSession: SparkSession = SparkSession
            .builder()
            .appName(this.getClass.getName)
            .master("local[*]")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()
        //获取数据
        val frame: DataFrame = sparkSession.read.parquet("gp23d_parquet")
        //注册临时视图
        frame.createOrReplaceTempView("log")

        import org.apache.spark.sql.functions._
        import sparkSession.implicits._
        val queryDF: DataFrame = frame
            .groupBy("provincename", "cityname")
            .count()
            .select("count", "provincename", "cityname")
        queryDF.show()
        //        queryDF.write
        //            .partitionBy("provincename","cityname")
        //            .json("province-city")
        val config: Config = ConfigFactory.load()
        val properties = new Properties()
        properties.put("user",config.getString("jdbc.user"))
        properties.put("password",config.getString("jdbc.password"))
        queryDF.write.mode(SaveMode.Append).jdbc(config.getString("jdbc.url"),config.getString("jdbc.tableName"),properties)

        sparkSession.sql(
            """|select
               |provincename,
               |cityname,
               |count(tmp.yuanshi) as originReq,
               |count(tmp.youxiao) as effectReq,
               |count(tmp.qingqiu) as adReq,
               |count(tmp.canyujingjia) as presentCnt,
               |count(tmp.success) as successCnt,
               |count(tmp.show) as showCnt,
               |count(tmp.click) as clickCnt,
               |sum(tmp.winprice)/1000 as dspconsum,
               |sum(tmp.adpayment)/1000 as dspcost
               |from
               |(
               |select
               |provincename,
               |cityname,
               |case when requestmode = 1 and processnode >=1 then 1 end as yuanshi,
               |case when requestmode = 1 and processnode >=2 then 1 end as youxiao,
               |case when requestmode = 1 and processnode = 3 then 1 end as qingqiu,
               |case when iseffective = "1" and isbilling = "1" and isbid = "1" then 1 end as canyujingjia,
               |case when iseffective = "1" and isbilling = "1" and iswin = "1" and adorderid <> 0 then 1 end as success,
               |case when requestmode = 2 and iseffective = "1" then 1 end as show,
               |case when requestmode = 3 and iseffective = "1" then 1 end as click,
               |case when iseffective = "1" and isbilling = "1" and iswin = "1" then winprice else 0 end as winprice,
               |case when iseffective = "1" and isbilling = "1" and iswin = "1" then adpayment else 0 end as adpayment
               |from log
               |) tmp
               |group by provincename, cityname""".stripMargin)
            .show()


        //dsl实现

        frame.select(
            $"provincename",
            $"cityname",
            when($"requestmode" === 1 and $"processnode" >= 1, 1).as("orgin"),
            when($"requestmode" === 1 and $"processnode" >= 2, 1) as "effective",
            when($"requestmode" === 1 and $"processnode" === 3, 1) as "request",
            when($"iseffective" === "1" and $"isbilling" === "1" and $"isbid" === "1", 1) as "canyujingjia",
            when($"iseffective" === "1" and $"isbilling" === "1" and $"iswin" === "1" and $"adorderid" =!= 0, 1) as
                "success",
            when($"requestmode" === 2 and $"iseffective" === "1", 1) as "show",
            when($"requestmode" === 3 and $"iseffective" === "1", 1) as "click",
            when($"iseffective" === "1" and $"isbilling" === "1" and $"iswin" === "1", $"winprice").otherwise(0) as
                "winprice",
            when($"iseffective" === "1" and $"isbilling" === "1" and $"iswin" === "1", $"adpayment")
                .otherwise(0) as "adpayment"
        )
            //            .select($"provincename",$"cityname",$"origin",$"effective",$"request",$"canyujingjia",$"success",$"show",
            //                $"click",$"winprice",$"adpayment")
            .groupBy($"provincename", $"cityname")
            .agg(
                count("orgin") as "origin",
                count("effective") as "effective",
                count("request") as "request",
                count("canyujingjia") as "presentCnt",
                count("success") as "successCnt",
                count("show") as "showCnt",
                count("click") as "clickCnt",
                sum("winprice") / 1000 as "dspconsume",
                sum("adpayment") / 1000 as "dspcost"
            ).show()


        sparkSession.stop()
    }
}
