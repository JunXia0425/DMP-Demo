package com.label

import org.apache.spark.sql.Row

import scala.collection.mutable

object AppLabel extends Tag {
    override def tag(row: Row): mutable.HashMap[String, Int] = {
        val appname: String = row.getAs[String]("appname")
        mutable.HashMap("APP" + appname -> 1)
    }

}
