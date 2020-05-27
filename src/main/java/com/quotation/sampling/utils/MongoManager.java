package com.quotation.sampling.utils;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;
import com.quotation.sampling.config.InitSetting;
import com.quotation.sampling.config.SamplingConfig;
import lombok.extern.java.Log;

import java.io.Serializable;
import java.util.Objects;

/**
 * @Classname MongoManager
 * @Description Mongo初始化连接池
 * @Date 2019/12/19 13:45
 * @Created by zqw
 * @Version 1.0
 */
public class MongoManager {

    private final static int POOL_SIZE = 1000;// 连接数量
    private final static int BLOCK_SIZE = 5000; // 等待队列长度

    private static MongoClient mongoClient= null;
    private MongoManager() {}


//    static {
//        initMongoDB();
//    }

    public static MongoDatabase getDatabase(String dbName) {
        return mongoClient.getDatabase(dbName);
    }

    public static MongoClient getMongoClient() {
        if(Objects.isNull(mongoClient)){
            MongoClientOptions mco = MongoClientOptions.builder()
                    .connectionsPerHost(POOL_SIZE)
                    .threadsAllowedToBlockForConnectionMultiplier(BLOCK_SIZE)
                    .build();
            return new MongoClient(new ServerAddress("192.168.214.193", 27017), mco);
        } else {
            return mongoClient;
        }
    }

    /**
     * 初始化连接池
     */
    public static void initMongoDB() {
        try {
            MongoClientOptions mco = MongoClientOptions.builder()
                    .connectionsPerHost(POOL_SIZE)
                    .threadsAllowedToBlockForConnectionMultiplier(BLOCK_SIZE)
                    .build();
            mongoClient = new MongoClient(new ServerAddress("192.168.214.193", 27017), mco);
        } catch (MongoException e) {
            e.printStackTrace();
        }
    }
}
