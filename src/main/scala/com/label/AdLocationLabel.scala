package com.label

import com.util.Util
import org.apache.spark.sql.Row

import scala.collection.mutable

object AdLocationLabel extends Tag {
    override def tag(row: Row): List[(String, Int)] = {
        val adSpaceType: Int = row.getAs[Int]("adspacetype")
        //补全
        val filledNum: String = Util.fillNum(adSpaceType)
        mutable.HashMap("LC"+filledNum -> 1).toList
    }
}
