package com.quotation.sampling.utils;

import com.google.common.collect.Lists;
import com.quotation.sampling.config.Constant;
import com.quotation.sampling.config.SamplingConfig;
import com.quotation.sampling.utils.JedisUtils;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.Data;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @Classname RabbitConnetionTools
 * @Description Rabbit 连接工具
 * @Date 2020/5/9 下午2:21
 * @Created by zqw
 * @Version 1.0
 */
@Data
public class RabbitConnectTools {

    private RMQConnectionConfig connectionConfig;
    private String exchange;

    public RabbitConnectTools(SamplingConfig samplingConfig) {
        this.connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(samplingConfig.getMqHost())
                .setPort(samplingConfig.getMqPort())
                .setVirtualHost("/")
                .setUserName(samplingConfig.getMqUserName())
                .setPassword(samplingConfig.getMqPassword())
                .build();
        this.exchange = samplingConfig.getExchange();
    }

    public void subscribeSource() throws Exception {
        ConnectionFactory factory = this.connectionConfig.getConnectionFactory();
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        //循环创建队列, 并绑定exchange
        String QUEUE_NAME = "flink_queue";
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        for (String code : queryAllFuture()) {
            channel.queueBind(QUEUE_NAME, this.exchange, code);
        }
        channel.close();
        connection.close();
    }

//    public void publishMessage() throws Exception {
//        ConnectionFactory factory = this.connectionConfig.getConnectionFactory();
//        Connection connection = factory.newConnection();
//        Channel channel = connection.createChannel();
//
//        channel.exchangeDeclare(Constant.REAL_MIN1, "direct", true);
//        channel.basicPublish("default", "default", null, "23423523542".getBytes());
//        channel.close();
//        connection.close();
//    }

    private List<String> queryAllFuture(){
        Set<String> allFutureCode = JedisUtils.getAllFutureCode();
        if(Objects.isNull(allFutureCode) || allFutureCode.isEmpty()) return Lists.newArrayList();
        return allFutureCode.stream()
                .filter(x -> x.split("_").length == 2)
                .map(x -> x.split("_")[1])
                .collect(Collectors.toList());
    }

}
