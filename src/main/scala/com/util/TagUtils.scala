package com.util

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row


object TagUtils {
//获取用户ID
    def getOneUserId(row:Row):String={
        row match {
            case t if StringUtils.isNotBlank(t.getAs[String]("imei")) => t.getAs[String]("imei")
            case t if StringUtils.isNotBlank(t.getAs[String]("mac")) => t.getAs[String]("mac")
            case t if StringUtils.isNotBlank(t.getAs[String]("idfa")) => t.getAs[String]("idfa")
            case t if StringUtils.isNotBlank(t.getAs[String]("openudid")) => t.getAs[String]("openudid")
            case t if StringUtils.isNotBlank(t.getAs[String]("androidid")) => t.getAs[String]("androidid")
            case t if StringUtils.isNotBlank(t.getAs[String]("imeimd5")) => t.getAs[String]("imeimd5")
            case t if StringUtils.isNotBlank(t.getAs[String]("macmd5")) => t.getAs[String]("macmd5")
            case t if StringUtils.isNotBlank(t.getAs[String]("idfamd5")) => t.getAs[String]("idfamd5")
            case t if StringUtils.isNotBlank(t.getAs[String]("openudidmd5")) => t.getAs[String]("openudidmd5")
            case t if StringUtils.isNotBlank(t.getAs[String]("androididmd5")) => t.getAs[String]("androididmd5")
            case t if StringUtils.isNotBlank(t.getAs[String]("imeisha1")) => t.getAs[String]("imeisha1")
            case t if StringUtils.isNotBlank(t.getAs[String]("macsha1")) => t.getAs[String]("macsha1")
            case t if StringUtils.isNotBlank(t.getAs[String]("idfasha1")) => t.getAs[String]("idfasha1")
            case t if StringUtils.isNotBlank(t.getAs[String]("openudidsha1")) => t.getAs[String]("openudidsha1")
            case t if StringUtils.isNotBlank(t.getAs[String]("androididsha1")) => t.getAs[String]("androidsha1")
            case _ => "其他"
        }
    }
}
