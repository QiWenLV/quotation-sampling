package com.quotation.sampling.operator;

import com.alibaba.fastjson.JSONObject;
import com.quotation.sampling.bean.KLine;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @Classname SourceMapFunction
 * @Description TODO
 * @Date 2020/5/9 下午4:01
 * @Created by zqw
 * @Version 1.0
 */
public class SourceMapFunction extends RichMapFunction<String, KLine> {
    @Override
    public KLine map(String value) throws Exception {
        JSONObject jsonObject = JSONObject.parseObject(value);
        String datetime = jsonObject.getString("datetime");
        if (datetime.length() <= 20) {
            datetime = datetime + ".000000";
        }
        LocalDateTime parse = LocalDateTime.parse(datetime, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS"));
        return KLine.builder()
                .code(jsonObject.getString("symbol"))
                .exchange(jsonObject.getString("exchange"))
                .datetime(parse)
                .updatetime(parse)
                .close(jsonObject.getDoubleValue("last_price"))
                .volume(jsonObject.getIntValue("volume"))
                .build();
    }
}
