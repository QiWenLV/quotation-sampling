package com.quotation.sampling;

import cn.hutool.core.bean.BeanUtil;
import com.google.common.collect.Lists;
import com.quotation.sampling.bean.KLine;
import com.quotation.sampling.config.Constant;
import com.quotation.sampling.config.InitSetting;
import com.quotation.sampling.config.SamplingConfig;
import com.quotation.sampling.operator.*;
import com.quotation.sampling.utils.RabbitConnectTools;
import com.quotation.sampling.utils.TimeHandler;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

/**
 * @Classname Start
 * @Description 行情重采样启动类
 * @Date 2020/5/8 下午4:14
 * @Created by zqw
 * @Version 1.0
 */
public class Start implements Serializable {

    public final static String RABBIT_QUEUE_NAME = "flink_queue";
    public final static String SEPARATOR = "_";
    private final static Map<String, OutputTag<KLine>> OUTPUT_TAGS = new HashMap<>();
    static {
        OUTPUT_TAGS.put("min1", new OutputTag<KLine>("min1"){});
        OUTPUT_TAGS.put("min5", new OutputTag<KLine>("min5"){});
        OUTPUT_TAGS.put("min15", new OutputTag<KLine>("min15"){});
        OUTPUT_TAGS.put("min30", new OutputTag<KLine>("min30"){});
        OUTPUT_TAGS.put("min60", new OutputTag<KLine>("min60"){});
    }

    /**
     * 入口
     * @param args  启动参数 -env dev
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        //基础配置
        ParameterTool parameters = ParameterTool.fromArgs(args);
        SamplingConfig samplingConfig = InitSetting.initSetting(parameters.get("env", "dev"));
        //flink运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //rabbit连接初始化
        RabbitConnectTools rabbitConnectTools = new RabbitConnectTools(samplingConfig);
        rabbitConnectTools.subscribeSource();
        RMQConnectionConfig rabbitConfig = rabbitConnectTools.getConnectionConfig();
        //数据处理
        env.getConfig().setGlobalJobParameters(parameters);
        SingleOutputStreamOperator<KLine> process = env
                .addSource(new RMQSource<String>(rabbitConfig, RABBIT_QUEUE_NAME, true, new SimpleStringSchema()))
                .map(new SourceMapFunction())
//                .filter(value -> TimeHandler.isBetweenTradingTime(value.getDatetime().toLocalTime()))
                .assignTimestampsAndWatermarks(new SamplingTimeWatermarks())
                .keyBy(value -> buildKey(value.getExchange(), value.getCode()))
                .window(new SamplingTimeWindows())
                .process(new SamplingProcessFunction(OUTPUT_TAGS));
        //输出一分钟线
        sink(process, Constant.MIN1, Constant.REAL_MIN1, rabbitConfig);
        sink(process, Constant.MIN5, Constant.REAL_MIN5, rabbitConfig);
        SingleOutputStreamOperator<KLine> process1 = process.keyBy(value ->  buildKey(value.getExchange(), value.getCode()))
                .window(new SamplingTimeWindows(Lists.newArrayList(15 * 60 * 1000L, 30 * 60 * 1000L, 60 * 60 * 1000L)))
                .process(new SamplingProcessFunction(OUTPUT_TAGS));
        sink(process1, Constant.MIN15, Constant.REAL_MIN15, rabbitConfig);
        sink(process1, Constant.MIN30, Constant.REAL_MIN30, rabbitConfig);
        sink(process1, Constant.MIN60, Constant.REAL_MIN60, rabbitConfig);
        env.execute("quotation sampling demo");
    }

    public static void sink(SingleOutputStreamOperator<KLine> process, String outputType, String sinkName, RMQConnectionConfig rabbitConfig){
        SimpleStringSchema simpleStringSchema = new SimpleStringSchema();
        process.getSideOutput(OUTPUT_TAGS.get(outputType)).addSink(new MongoSinkFunction(sinkName));
        process.getSideOutput(OUTPUT_TAGS.get(outputType)).addSink(new RabbitSinkFunction(rabbitConfig, sinkName, simpleStringSchema));
    }

    public static String buildKey(String...args){
        StringJoiner stringJoiner = new StringJoiner(SEPARATOR);
        for (String arg : args) {
            stringJoiner.add(arg);
        }
        return stringJoiner.toString();
    }
}
