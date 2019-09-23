package com.util

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.typesafe.config.ConfigFactory

import scala.collection.mutable.ListBuffer


/**
  * 从高德地图获取商圈信息
  */
object AmapUtil {
    /**
      * 解析经纬度
      *
      * @param long 经度
      * @param lat 纬度
      * @return
      */
    def getBusinessFromAmap(lat: Double, long: Double): String = {
        val areas = ListBuffer[String]()
        val key: String = ConfigFactory.load().getString("gaode.key")


        val url = s"https://restapi.amap.com/v3/geocode/regeo?" +
            s"location=${long},${lat}&key=${key}&radius=1000&extensions=all"

        //调用Http请求
        val jsonStr: String = HttpUtil.get(url)
        //解析json字符串
        val js: JSONObject = JSON.parseObject(jsonStr)
        //判断当前状态是否为1
        val status: Int = js.getIntValue("status")
        if (status != 1) return ""

        val regeoCode: JSONObject = js.getJSONObject("regeocode")
        if (regeoCode == null) return ""
        val addressComponent: JSONObject = regeoCode.getJSONObject("addressComponent")
        if (addressComponent == null) return ""
        val businessAreas: JSONArray = addressComponent.getJSONArray("businessAreas")
        if (businessAreas == null) return ""
        //循环数组
        for (elem <- businessAreas.toArray) {
            if(elem.isInstanceOf[JSONObject]){

                val area: JSONObject = elem.asInstanceOf[JSONObject]
                val name: String = area.getString("name")
                areas += name
            }
        }
        println("商圈信息："+areas.mkString(","))
        areas.mkString(",")

    }

    def main(args: Array[String]): Unit = {
        val str: String = getBusinessFromAmap(116.310003, 39.991957)
        println(str)
    }

}
