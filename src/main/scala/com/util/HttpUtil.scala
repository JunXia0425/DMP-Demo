package com.util

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils

/**
  * Http请求协议 GET请求
  */
object HttpUtil {
    /**
      * GET请求
      * @param url
      * @return json字符串
      */
    def get(url:String): String = {
        val client: CloseableHttpClient = HttpClients.createDefault()
        val httpGet = new HttpGet(url)
        //发送请求
        val response: CloseableHttpResponse = client.execute(httpGet)
        //处理返回结果
        EntityUtils.toString(response.getEntity,"UTF-8")

    }

    def main(args: Array[String]): Unit = {
        val config: Config = ConfigFactory.load()
        val url = "https://restapi.amap.com/v3/geocode/regeo?location=116.310003," +
            s"39.991957&key=${config.getString("gaode.key")}&radius=1000&extensions=all"

        val str: String = HttpUtil.get(url)

        println(str)
    }
}
