package com.etl


import com.util.{SchemaUtil, String2Type}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.Random

/**
  * 进行数据格式转换
  */
object Log2parquet {

    //京城 long=（116.20,116.52) lat = (39.83,40)
    def changePos(x: Boolean): String = {
        val random = new Random()
        var pos = 40.0
        if (x) {
            pos = Math.random() * (116.52 - 116.20) + 116.52
            //            while (pos < 116.20 || pos > 116.52) {
            //                Math.random()
            //            }
        } else {
            pos = Math.random() * (40 - 39.83) + 40
            //            while (pos < 39.83 || pos > 40) {
            //                pos = random.nextDouble()
            //            }
        }
        pos.toString
    }

    def main(args: Array[String]): Unit = {
        //设定目录限制
        if (args.length != 2) {
            println("目录不正确，退出程序")
            sys.exit()
        }
        //获取目录参数
        val Array(inputPath, outputPath) = args

        val session: SparkSession = SparkSession.builder()
            .appName(this.getClass.getName)
            .master("local[*]")
            //设置序列化级别&压缩方式
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .getOrCreate()


        val sc: SparkContext = session.sparkContext

        val lines: RDD[String] = sc.textFile(inputPath)

        //设置过滤条件和切分条件，内部如果切割条件相连过多，那么 需要设置切割处理条件
        //                val count: Long = lines.map(t=>t.split(",",-1)).filter(t=>t.length>=85).count()


        val rowRDD = lines.map(t => t.split(",", -1)).filter(t => t.length >= 85).map(arr => {
            println(s"long=${arr(22)},lat=${arr(23)}")
            Row(
                arr(0),
                String2Type.toInt(arr(1)),
                String2Type.toInt(arr(2)),
                String2Type.toInt(arr(3)),
                String2Type.toInt(arr(4)),
                arr(5),
                arr(6),
                String2Type.toInt(arr(7)),
                String2Type.toInt(arr(8)),
                String2Type.toDouble(arr(9)),
                String2Type.toDouble(arr(10)),
                arr(11),
                arr(12),
                arr(13),
                arr(14),
                arr(15),
                arr(16),
                String2Type.toInt(arr(17)),
                arr(18),
                arr(19),
                String2Type.toInt(arr(20)),
                String2Type.toInt(arr(21)),
                changePos(true),
                changePos(false),
                arr(24),
                arr(25),
                String2Type.toInt(arr(26)),
                arr(27),
                String2Type.toInt(arr(28)),
                arr(29),
                String2Type.toInt(arr(30)),
                String2Type.toInt(arr(31)),
                String2Type.toInt(arr(32)),
                arr(33),
                String2Type.toInt(arr(34)),
                String2Type.toInt(arr(35)),
                String2Type.toInt(arr(36)),
                arr(37),
                String2Type.toInt(arr(38)),
                String2Type.toInt(arr(39)),
                String2Type.toDouble(arr(40)),
                String2Type.toDouble(arr(41)),
                String2Type.toInt(arr(42)),
                arr(43),
                String2Type.toDouble(arr(44)),
                String2Type.toDouble(arr(45)),
                arr(46),
                arr(47),
                arr(48),
                arr(49),
                arr(50),
                arr(51),
                arr(52),
                arr(53),
                arr(54),
                arr(55),
                arr(56),
                String2Type.toInt(arr(57)),
                String2Type.toDouble(arr(58)),
                String2Type.toInt(arr(59)),
                String2Type.toInt(arr(60)),
                arr(61),
                arr(62),
                arr(63),
                arr(64),
                arr(65),
                arr(66),
                arr(67),
                arr(68),
                arr(69),
                arr(70),
                arr(71),
                arr(72),
                String2Type.toInt(arr(73)),
                String2Type.toDouble(arr(74)),
                String2Type.toDouble(arr(75)),
                String2Type.toDouble(arr(76)),
                String2Type.toDouble(arr(77)),
                String2Type.toDouble(arr(78)),
                arr(79),
                arr(80),
                arr(81),
                arr(82),
                arr(83),
                String2Type.toInt(arr(84))
            )
        })
        val dataFrame: DataFrame = session.createDataFrame(rowRDD, SchemaUtil.structType)


        dataFrame.write.parquet(outputPath)
        //                println(count)
    }
}
