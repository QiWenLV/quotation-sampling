package com.quotation.sampling.utils;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.io.Serializable;

/**
 * @Classname MongoHandler
 * @Description Mongo 连接辅助工具
 * @Date 2019/12/19 14:03
 * @Created by zqw
 * @Version 1.0
 */
public class MongoHandler implements Serializable {

    private MongoClient mongoClient;
    private String dbName;
    private String collectionName;

    public MongoHandler(MongoClient mongoClient, String dbName, String collectionName) {
        this.mongoClient = mongoClient;
        this.dbName = dbName;
        this.collectionName = collectionName;
    }

    public void saveJson(String json){
        MongoCollection<Document> collection = mongoClient.getDatabase(dbName).getCollection(collectionName);
        collection.insertOne(Document.parse(json));
    }
}
