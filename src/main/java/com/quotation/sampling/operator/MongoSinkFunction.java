package com.quotation.sampling.operator;

import cn.hutool.core.bean.BeanUtil;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.quotation.sampling.bean.KLine;
import com.quotation.sampling.config.InitSetting;
import com.quotation.sampling.config.SamplingConfig;
import com.quotation.sampling.utils.MongoManager;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bson.Document;

import java.time.format.DateTimeFormatter;
import java.util.Map;

/**
 * @Classname MongoSinkFunction
 * @Description Mongo sink
 * @Date 2020/5/9 下午3:49
 * @Created by zqw
 * @Version 1.0
 */
public class MongoSinkFunction extends RichSinkFunction<KLine> {

    private String collectionName;
    private String dbName;
    private static MongoClient mongoClient;

    public MongoSinkFunction(String collectionName) {
        this.collectionName = collectionName;
        mongoClient = MongoManager.getMongoClient();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool param = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        SamplingConfig samplingConfig = InitSetting.initSetting(param.get("env"));
        this.dbName = samplingConfig.getDbName();
    }

    @Override
    public void invoke(KLine value, Context context) throws Exception {
        Document document = new Document();
        Map<String, Object> targetMap = BeanUtil.beanToMap(value);
        targetMap.put("datetime", value.getDatetime().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        document.putAll(targetMap);
        saveDocument(document);
    }

    public void saveDocument(Document document){
        MongoCollection<Document> collection = mongoClient.getDatabase(dbName).getCollection(collectionName);
        collection.insertOne(document);
    }
}
