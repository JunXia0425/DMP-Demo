package com.label

import org.apache.spark.sql.Row

import scala.collection.mutable

object AreaLabel extends Tag {
    override def tag(row: Row): mutable.HashMap[String, Int] = {
        val proviceName: String = row.getAs[String]("provincename")
        val cityname: String = row.getAs[String]("cityname")
        mutable.HashMap("ZP" + proviceName -> 1, "ZC" + cityname -> 1)
    }
}
