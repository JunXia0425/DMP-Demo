package com.label

import org.apache.spark.sql.Row

import scala.collection.mutable

object DeviceLabel extends Tag {
    override def tag(row: Row): List[(String, Int)] = {
        val clientOS: Int = row.getAs[Int]("client")
        val ispname: String = row.getAs[String]("ispname")
        val networkmannername: String = row.getAs[String]("networkmannername")

        val os = clientOS match {
            case 1 => "D00010001"
            case 2 => "D00010002"
            case 3 => "D00010003"
            case _ => "D00010004"
        }

        val network = networkmannername match {
            case "WIFI" => "D00020001"
            case "4G" => "D00020002"
            case "3G" => "D00020003"
            case "2G" => "D00020004"
            case _ => "D00020005"
        }

        val isp = ispname match {
            case "移动" => "D00030001"
            case "联通" => "D00030002"
            case "电信" => "D00030003"
            case _ => "D00030004"
        }

        mutable.HashMap(os -> 1, network -> 1, isp -> 1).toList
    }
}
