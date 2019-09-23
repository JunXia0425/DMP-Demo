package com.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory}



object HbaseUtil {
    def getConnection(configuration: Configuration): Connection = {
        //创建hadoop任务

        //配置连接
        configuration.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181")
        //获取连接
        ConnectionFactory.createConnection(configuration)
    }

    def createTable(connection: Connection,tableName: String,columnFamily:String): Unit ={
        val admin: Admin = connection.getAdmin
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            println("当前表可用")
            //创建表对象
            val table = new HTableDescriptor(TableName.valueOf(tableName))
            //创建列簇
            val columnDescriptor = new HColumnDescriptor(columnFamily)
            //将列簇加入表中
            table.addFamily(columnDescriptor)
            admin.createTable(table)

            admin.close()
            connection.close()
        }
    }
}
