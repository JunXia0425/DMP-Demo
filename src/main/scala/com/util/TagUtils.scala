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

    def getAllUserId(row:Row):List[String]={
        var list = List[String]()
        if(StringUtils.isNotBlank(row.getAs[String]("imei"))) list:+="IM"+row.getAs[String]("imei")
        if(StringUtils.isNotBlank(row.getAs[String]("mac"))) list:+="MC"+row.getAs[String]("mac")
        if(StringUtils.isNotBlank(row.getAs[String]("idfa"))) list:+="ID"+row.getAs[String]("idfa")
        if(StringUtils.isNotBlank(row.getAs[String]("openudid"))) list:+="OD"+row.getAs[String]("openudid")
        if(StringUtils.isNotBlank(row.getAs[String]("androidid"))) list:+="AD"+row.getAs[String]("androidid")
        if(StringUtils.isNotBlank(row.getAs[String]("imeimd5"))) list:+="IM"+row.getAs[String]("imeimd5")
        if(StringUtils.isNotBlank(row.getAs[String]("macmd5"))) list:+="MC"+row.getAs[String]("macmd5")
        if(StringUtils.isNotBlank(row.getAs[String]("idfamd5"))) list:+="ID"+row.getAs[String]("idfamd5")
        if(StringUtils.isNotBlank(row.getAs[String]("openudidmd5"))) list:+="OD"+row.getAs[String]("openudidmd5")
        if(StringUtils.isNotBlank(row.getAs[String]("androididmd5"))) list:+="AD"+row.getAs[String]("androididmd5")
        if(StringUtils.isNotBlank(row.getAs[String]("imeisha1"))) list:+="IM"+row.getAs[String]("imeisha1")
        if(StringUtils.isNotBlank(row.getAs[String]("macsha1"))) list:+="MC"+row.getAs[String]("macsha1")
        if(StringUtils.isNotBlank(row.getAs[String]("idfasha1"))) list:+="ID"+row.getAs[String]("idfasha1")
        if(StringUtils.isNotBlank(row.getAs[String]("openudidsha1"))) list:+="OD"+row.getAs[String]("openudidsha1")
        if(StringUtils.isNotBlank(row.getAs[String]("androididsha1"))) list:+="AD"+row.getAs[String]("androididsha1")
        list
    }
}
