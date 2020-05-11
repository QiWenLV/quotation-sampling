package com.quotation.sampling.utils;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;

/**
 * @Classname MongoManager
 * @Description Mongo初始化连接池
 * @Date 2019/12/19 13:45
 * @Created by zqw
 * @Version 1.0
 */
public class MongoManager {

    private static MongoClient mongoClient= null;
    private MongoManager() { }

    static {
        initDBPrompties();
    }

    public static MongoDatabase getDatabase(String dbName) {
        return mongoClient.getDatabase(dbName);
    }

    public static MongoClient getClient(){
        return mongoClient;
    }

    /**
     * 初始化连接池
     */
    private static void initDBPrompties() {
        try {
            MongoClientOptions mco = MongoClientOptions.builder()
                    .connectionsPerHost(100)
                    .threadsAllowedToBlockForConnectionMultiplier(100)
                    .build();
            mongoClient = new MongoClient(new ServerAddress("192.168.214.193", 27017), mco);
        } catch (MongoException e) {
            e.printStackTrace();
        }
    }
}
