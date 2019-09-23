package com.label

import org.apache.spark.sql.Row

import scala.collection.mutable

object ChannelLabel extends Tag {
    override def tag(row: Row): List[(String, Int)] = {
        val channelId: Int = row.getAs[Int]("adplatformproviderid")
        mutable.HashMap("CN" + channelId -> 1).toList
    }
}
