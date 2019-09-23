package com.exam

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.exam.tag.TypeTag
import com.exam.util.JsonUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Exam {
    def main(args: Array[String]): Unit = {
        if(args.length!=3){
            println("路径有误，程序退出")
            sys.exit()
        }

        val Array(inputPath,outputPath1,outputPath2) = args

        val sparkSession: SparkSession = SparkSession
            .builder()
            .appName(this.getClass.getName)
            .master("local[1]")
            .getOrCreate()

        import sparkSession.implicits._
        val frame: DataFrame = sparkSession.read.text(inputPath)

        val data: Dataset[List[(String, String)]] = frame.map(row => {
            JsonUtil.getPOIS(row)
        })

//        val cached: Dataset[List[(String, String, Int)]] = cached.persist()

        val businessAreas: RDD[List[String]] = data.rdd.map(_.map(_._1))

        val strings: List[String] = businessAreas.reduce((l1,l2)=>l1:::l2)

        sparkSession.sparkContext.parallelize(strings).map((_,1)).reduceByKey(_+_).saveAsTextFile(outputPath1)

        //2.

        val types: List[String] = data.rdd.map(_.map(_._2)).reduce((l1,l2)=>l1:::l2)

        var typeList = List[(String,Int)]()
        for (elem <- types) {
           typeList :::= TypeTag.makeTag(elem)
        }

        sparkSession.sparkContext.parallelize(typeList).reduceByKey(_+_).saveAsTextFile(outputPath2)


    }

}
