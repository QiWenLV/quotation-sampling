package com.quotation.sampling.operator;

import com.alibaba.fastjson.JSON;
import com.quotation.sampling.bean.KLine;
import com.quotation.sampling.utils.MongoHandler;
import com.quotation.sampling.utils.MongoManager;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @Classname MongoSinkFunction
 * @Description Mongo sink
 * @Date 2020/5/9 下午3:49
 * @Created by zqw
 * @Version 1.0
 */
public class MongoSinkFunction extends RichSinkFunction<KLine> {

    private MongoHandler mongoHandler;
    private String collectionName;

    private final static String DB_NAME = "quantaxis2";

    public MongoSinkFunction(String collectionName) {
        this.collectionName = collectionName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.mongoHandler = new MongoHandler(MongoManager.getClient(), DB_NAME, this.collectionName);
    }

    @Override
    public void invoke(KLine value, Context context) throws Exception {
        this.mongoHandler.saveJson(JSON.toJSONString(value));
    }
}
