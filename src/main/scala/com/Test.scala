package com

import com.etl.Log2parquet

object Test {
    def main(args: Array[String]): Unit = {
        println(Log2parquet.changePos(true))
        println(Log2parquet.changePos(false))
    }

}
