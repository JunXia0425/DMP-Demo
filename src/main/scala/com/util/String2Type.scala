package com.util

/**
  * 类型工具类
  */
object String2Type {

    def toInt(string: String): Int = {
        try {
            string.toInt
        } catch {
            case _: Exception => 0
        }
    }

    def toDouble(string: String): Double = {
        try {
            string.toDouble
        } catch {
            case _: Exception => 0.0
        }
    }

//    def toDouble2(string: String):Double={
//
//    }
}
