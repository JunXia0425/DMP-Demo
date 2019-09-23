package com.label

import com.util.Util
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object KeyWordLabel extends Tag {
    override def tag(row: Row): List[(String, Int)] = {
        val keyWords: String = row.getAs[String]("keywords")

        var keyWordList: ListBuffer[String] = new ListBuffer[String]
        // 关键字不为空
        if (StringUtils.isNotBlank(keyWords)) keyWordList = Util.getKeyWords(keyWords)

        val res = mutable.HashMap[String, Int]()

        for (elem <- keyWordList) {
            res.put("K" + elem, 1)
        }
        res.toList
    }
}
