package com.label

import org.apache.spark.sql.Row

import scala.collection.mutable

trait Tag {
    /**
      * 为数据打标签
      * @param row
      * @return list
      */
    def tag(row: Row): List[(String, Int)]
}
