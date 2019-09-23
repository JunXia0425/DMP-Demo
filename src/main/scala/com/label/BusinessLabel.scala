package com.label

import ch.hsr.geohash.GeoHash
import com.util.{AmapUtil, RedisUtil, String2Type}
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object BusinessLabel extends Tag {
    override def tag(row: Row): mutable.HashMap[String, Int] = {
        val long: Double = String2Type.toDouble(row.getAs[String]("long"))
        val lat: Double = String2Type.toDouble(row.getAs[String]("lat"))
        val map = new mutable.HashMap[String, Int]()
        if (checkGeographic(long, lat)) {
            val str: String = getBusiness(long, lat)
            val list = ListBuffer[String]()
            if (StringUtils.isNotBlank(str)) {
                val arr: Array[String] = str.split(",")
                list.appendAll(arr)
            }
            for (elem <- list) {
                map.put(elem, 1)
            }
        }
        map
    }

    /**
      * 获取商圈信息。先去查询redis，如果redis没有请求api
      *
      * @param long 经度
      * @param lat 纬度
      */
    def getBusiness(lat: Double, long: Double): String = {
        //GeoHash
        val geohashStr: String = GeoHash.geoHashStringWithCharacterPrecision(lat, long, 6)
        var business: String = getBusinessFromRedis(geohashStr)
        if (business == null) {
            business = AmapUtil.getBusinessFromAmap(lat, long)
            setBusinessToRedis(geohashStr, business)
        }
        business
    }

    /**
      * 从redis获获取商圈geoHash
      *
      * @param geoHashStr
      * @return
      */
    def getBusinessFromRedis(geoHashStr: String): String = {
        val jedis: Jedis = RedisUtil.getConnection()

        val business: String = jedis.get(geoHashStr)
        jedis.close()
        business
    }

    /**
      * 将geohash作key，business作value传入redis
      *
      * @param key
      * @param value
      */
    def setBusinessToRedis(key: String, value: String): Unit = {
        val jedis: Jedis = RedisUtil.getConnection()
        jedis.set(key, value)
        jedis.close()
    }

    def checkGeographic(lat: Double, long: Double): Boolean = {
        long >= 73 & long <= 136 & lat >= 3 & lat < 53
    }

}
