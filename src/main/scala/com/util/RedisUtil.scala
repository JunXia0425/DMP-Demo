package com.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * redis工具类
  */
object RedisUtil {
    private val config = new JedisPoolConfig
    config.setMaxTotal(20)
    config.setMaxIdle(10)

    private val pool = new JedisPool(config, "hadoop01", 6379, 10 * 1000)

    def getConnection(): Jedis = {
        pool.getResource
    }

}
