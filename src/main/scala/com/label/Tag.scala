package com.label

import org.apache.spark.sql.Row

import scala.collection.mutable

trait Tag {
    /**
      * 为数据打标签
      * @param row
      * @return hashmap
      */
    def tag(row: Row): mutable.HashMap[String, Int]
}
