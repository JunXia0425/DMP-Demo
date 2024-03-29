package com.util

/**
  * 处理指标统计工具类
  */
object RptUtil {
    //处理请求数
    def reqPt(requestmode: Int, processnode: Int): List[Double] = {
        if (requestmode == 1 && processnode == 1) {
            //原始请求，有效请求，广告请求
            List[Double](1, 0, 0)
        } else if (requestmode == 1 && processnode == 2) {
            List[Double](1, 1, 0)
        } else if (requestmode == 1 && processnode == 3) {
            List[Double](1, 1, 1)
        } else {
            List[Double](0, 0, 0)
        }
    }

    //处理点击，展示数
    def clickPt(requestmode: Int, iseffective: Int): List[Double] = {
        if (requestmode == 2 && iseffective == 1) {
            List[Double](1, 0)
        } else if (requestmode == 3 && iseffective == 1) {
            List[Double](0, 1)
        } else {
            List[Double](0, 0)
        }
    }

    //处理参与、竞价成功数，
    def adPt(iseffective: Int, isbilling: Int,
               isbid: Int, iswin: Int, adorderid: Int,
               winprice: Double, adpayment: Double): List[Double] = {
        if (iseffective == 1 && isbilling == 1) {
            if (isbid == 1) {
                List[Double](1, 0, 0, 0)
            } else if (iswin == 1) {
                if (adorderid != 0) {
                    List[Double](0, 1, 0, 0)
                } else {
                    List[Double](0, 0, winprice / 1000.0, adpayment / 1000.0)
                }
            } else {
                List[Double](0, 0, 0, 0)
            }
        } else {
            List[Double](0, 0, 0, 0)
        }
    }
}
