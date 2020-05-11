package com.quotation.sampling.operator;

import com.quotation.sampling.bean.KLine;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
import java.time.ZoneOffset;

/**
 * @Classname SamplingTimeWatermarks
 * @Description TODO
 * @Date 2020/5/9 下午4:02
 * @Created by zqw
 * @Version 1.0
 */
public class SamplingTimeWatermarks implements AssignerWithPunctuatedWatermarks<KLine> {
    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(KLine lastElement, long extractedTimestamp) {
        return new Watermark(lastElement.getDatetime().toInstant(ZoneOffset.of("+8")).toEpochMilli());
    }

    @Override
    public long extractTimestamp(KLine element, long previousElementTimestamp) {
        return element.getDatetime().toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }
}
