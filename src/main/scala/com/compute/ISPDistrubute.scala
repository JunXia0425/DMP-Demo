package com.compute

import org.apache.spark.sql.{DataFrame, SparkSession}

object ISPDistrubute {
    def main(args: Array[String]): Unit = {
        val sparkSession: SparkSession = SparkSession
            .builder()
            .appName(this.getClass.getName)
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .master("local[*]")
            .getOrCreate()
        if (args.length != 1) {
            println("目录不正确，退出程序")
            sys.exit()
        }
        val inputPath = args(0)

        val frame: DataFrame = sparkSession.read.parquet(inputPath)

        frame.createOrReplaceTempView("log")
        //sql实现
        sparkSession.sql(
            """select
              |ispname,
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
              |ispname,
              |case when requestmode = 1 and processnode >=1 then 1 end as yuanshi,
              |case when requestmode = 1 and processnode >=2 then 1 end as youxiao,
              |case when requestmode = 1 and processnode = 3 then 1 end as qingqiu,
              |case when iseffective = '1' and isbilling = '1' and isbid = '1' then 1 end as canyujingjia,
              |case when iseffective = '1' and isbilling = '1' and iswin = '1' and adorderid <> 0 then 1 end as success,
              |case when requestmode = 2 and iseffective = '1' then 1 end as show,
              |case when requestmode = 3 and iseffective = '1' then 1 end as click,
              |case when iseffective = '1' and isbilling = '1' and iswin = '1' then winprice else 0 end as winprice,
              |case when iseffective = '1' and isbilling = '1' and iswin = '1' then adpayment else 0 end as adpayment
              |from log
              |) tmp
              |group by ispname""".stripMargin)
            .show()
        sparkSession.stop()
    }
}
