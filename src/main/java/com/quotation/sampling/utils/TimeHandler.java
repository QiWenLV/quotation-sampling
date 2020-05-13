package com.quotation.sampling.utils;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple2;

import java.time.LocalTime;
import java.util.List;

/**
 * @Classname TimeHandler
 * @Description TODO
 * @Date 2020/5/11 下午2:36
 * @Created by zqw
 * @Version 1.0
 */
public class TimeHandler {

    public static final List<Tuple2<LocalTime, LocalTime>> defaultTradingTime = Lists.newArrayList(
            new Tuple2<>(LocalTime.of(9, 0, 0), LocalTime.of(11, 30, 0)),
            new Tuple2<>(LocalTime.of(13, 0, 0), LocalTime.of(15, 15, 0)),
            new Tuple2<>(LocalTime.of(21, 0, 0), LocalTime.of(23, 59, 59)),
            new Tuple2<>(LocalTime.of(0, 0, 0), LocalTime.of(2, 30, 0))
    );

    public static boolean isBetweenTradingTime(LocalTime localTime){
        for(Tuple2<LocalTime, LocalTime> time : defaultTradingTime){
            if(localTime.isAfter(time.f0) && localTime.isBefore(time.f1)){
                return true;
            }
        }
        return false;
    }
}
