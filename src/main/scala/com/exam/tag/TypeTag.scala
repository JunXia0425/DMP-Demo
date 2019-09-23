package com.exam.tag

object TypeTag {
    def makeTag(string: String):List[(String,Int)]={
        val strings: Array[String] = string.split(";")

        strings.map((_,1)).toList
    }
}
