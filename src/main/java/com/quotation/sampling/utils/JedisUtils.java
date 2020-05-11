package com.quotation.sampling.utils;


import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @Classname JedisUtils
 * @Description Jedis工具类
 * @Date 2019/11/21 15:00
 * @Created by zqw
 * @Version 1.0
 */
@Slf4j
public class JedisUtils {

    private static JedisPool jedisPool = null;

    private static void initJedisPool(){
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(10);
        jedisPool = new JedisPool(jedisPoolConfig,
                "192.168.214.181",
                32299,
                10000,
                null,
                0);
    }

    public static synchronized JedisPool getJedisPool(){
        if (null == jedisPool){
            initJedisPool();
        }
        return jedisPool;
    }

    public static Set<String> getAllFutureCode(){
        Jedis jedis = getJedisPool().getResource();
        Set<String> hkeys = jedis.hkeys("sec::info");
        jedis.close();
        return hkeys;
    }
}
