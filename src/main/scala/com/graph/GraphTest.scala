package com.graph

import org.apache.spark.graphx
import org.apache.spark.graphx.{Edge, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object GraphTest {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
            .appName("graph")
            .master("local")
            .getOrCreate()
        //创建点和边
        val vertexRDD: RDD[(Long, (String, Int))] = spark.sparkContext.makeRDD(
            Seq(
                (1L, ("小明", 26)),
                (2L, ("小红", 30)),
                (6L, ("小黑", 33)),
                (9L, ("小白", 26)),
                (133L, ("小黄", 30)),
                (138L, ("小蓝", 33)),
                (158L, ("小绿", 26)),
                (16L, ("小龙", 30)),
                (44L, ("小强", 33)),
                (21L, ("小胡", 26)),
                (5L, ("小狗", 30)),
                (7L, ("小熊", 33))
            )
        )

        //构造边的集合
        val eageRDD: RDD[Edge[Int]] = spark.sparkContext.makeRDD(Seq(
            Edge(1L, 133L, 0),
            Edge(2L, 133L, 0),
            Edge(6L, 133L, 0),
            Edge(9L, 133L, 0),
            Edge(6L, 138L, 0),
            Edge(16L, 138L, 0),
            Edge(16L, 138L, 0),
            Edge(44L, 138L, 0),
            Edge(5L, 158L, 0),
            Edge(7L, 158L, 0)
        ))
        //
        //        构建图
        val vertices: VertexRDD[VertexId] = graphx.Graph(vertexRDD, eageRDD).connectedComponents().vertices

        vertices.foreach(println)

        vertices.join(vertexRDD).map {
            case (userId, (cnId, (name, age))) => (cnId, List((name, age)))
        }
            .reduceByKey(_ ++ _)
            .foreach(println)
    }
}
