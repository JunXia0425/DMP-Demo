package com

import com.label.BusinessLabel
import com.util.TagUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

/**
  * 上下文标签主类
  */
object TagContext {
    def main(args: Array[String]): Unit = {
        if (args.length!=2) {
            println("目录不正确")
            sys.exit()
        }

        val Array(inputPath,outputPath) = args
        //创建spark上下文
        val sparkSession: SparkSession = SparkSession.builder()
            .appName("Tags")
            .master("local[*]")
            .getOrCreate()
        //读取数据文件
        val df: DataFrame = sparkSession.read.parquet(inputPath)

        //处理数据信息
        import sparkSession.implicits._
        df.map(row=>{
            val userId = TagUtils.getOneUserId(row)
            //接下来标签 实现
            val map: mutable.HashMap[String, Int] = BusinessLabel.tag(row)
            (userId,map)
        }).write.json(outputPath)

        sparkSession.stop()
    }
}
