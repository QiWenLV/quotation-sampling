package com.quotation.sampling.operator;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @Classname SamplingTimeWindows
 * @Description TODO
 * @Date 2020/5/8 下午6:30
 * @Created by zqw
 * @Version 1.0
 */
public class SamplingTimeWindows extends WindowAssigner<Object, TimeWindow> {

    private final ArrayList<Long> timeInterval;

    public SamplingTimeWindows() {
        this(Lists.newArrayList(60 * 1000L, 5 * 60 * 1000L));
    }

    public SamplingTimeWindows(ArrayList<Long> timeInterval) {
        if(Objects.isNull(timeInterval) || timeInterval.isEmpty()) {
            throw new RuntimeException("args error");
        }
        this.timeInterval = timeInterval;
    }

    @Override
    public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
        return timeInterval.stream()
                .map(x -> {
                    long lastStart = TimeWindow.getWindowStartWithOffset(timestamp, 0, x);
                    return new TimeWindow(lastStart, lastStart + x);
                }).collect(Collectors.toList());
    }

    @Override
    public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
        return EventTimeTrigger.create();
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return  new TimeWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return true;
    }
}
