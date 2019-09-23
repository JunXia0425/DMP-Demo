package com.util

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Util {
    /**
      * 小于10数值填充
      *
      * @param num
      * @return
      */
    def fillNum(num: Int): String = {
//        if (num / 10== 0) {
//            "0" + num
//        } else {
//            num.toString
//        }

        f"$num%02d"
    }

    /**
      * 从字符串中获取关键词
      * 判断字符串中是否含有'|'，含有则切分，保留符合条件的关键词，放入kwList；
      * 若不包含，则检查字符串是否符合条件，符合放入kwList
      *
      * @param keywordStr
      * @return
      */
    def getKeyWords(keywordStr: String): ListBuffer[String] = {
        val kwList = new ListBuffer[String]
        if (keywordStr.contains("|")) {
            val wordsArr = keywordStr.split("\\|")

            //保留符合长度关键词
            val filtedWords = wordsArr.filter(word => keywordLengthCheck(word))

            for (elem <- filtedWords) {
                kwList :+ elem
            }
        } else if (keywordLengthCheck(keywordStr)) {
            kwList :+ keywordStr
        }

        kwList
    }

    /**
      * 判断字符串是否符合关键词标准
      *
      * @param keyWord
      * @return
      */
    def keywordLengthCheck(keyWord: String): Boolean = {
        val length = keyWord.length
        length >= 3 && length <= 8
    }

    def flatHashMap(map: mutable.HashMap[String, Int]*): mutable.HashMap[String, Int] = {
        val bigMap = new mutable.HashMap[mutable.HashMap[String, Int], Int]

        for (elem <- map) {
            bigMap.put(elem, 1)
        }

        bigMap.flatMap(_._1)
    }



}
