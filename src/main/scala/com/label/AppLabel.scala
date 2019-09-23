package com.label

import org.apache.spark.sql.Row

import scala.collection.mutable

object AppLabel extends Tag {
    override def tag(row: Row): List[(String, Int)] = {
        val appname: String = row.getAs[String]("appname")
        mutable.HashMap("APP" + appname -> 1).toList
    }

}
