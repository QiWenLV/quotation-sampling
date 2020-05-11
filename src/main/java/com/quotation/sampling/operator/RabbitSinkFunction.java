package com.quotation.sampling.operator;

import com.alibaba.fastjson.JSON;
import com.quotation.sampling.bean.KLine;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @Classname RabbitSinkFunction
 * @Description Rabbit sink
 * @Date 2020/5/11 上午11:22
 * @Created by zqw
 * @Version 1.0
 */
public class RabbitSinkFunction extends RichSinkFunction<KLine> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RabbitSinkFunction.class);


    private final RMQConnectionConfig rmqConnectionConfig;
    protected final String exchange;
    protected transient Connection connection;
    protected transient Channel channel;
    protected SerializationSchema schema;
    private boolean logFailuresOnly = false;

    public RabbitSinkFunction(RMQConnectionConfig rmqConnectionConfig, String exchange, SerializationSchema schema) {
        this.rmqConnectionConfig = rmqConnectionConfig;
        this.exchange = exchange;
        this.schema = schema;
    }

    /**
     * Sets up the queue. The default implementation just declares the queue. The user may override
     * this method to have a custom setup for the queue (i.e. binding the queue to an exchange or
     * defining custom queue parameters)
     */
    protected void setupExchange() throws IOException {
        channel.exchangeDeclare(this.exchange, "direct", false, false, null);
    }

    /**
     * Defines whether the producer should fail on errors, or only log them.
     * If this is set to true, then exceptions will be only logged, if set to false,
     * exceptions will be eventually thrown and cause the streaming program to
     * fail (and enter recovery).
     *
     * @param logFailuresOnly The flag to indicate logging-only on exceptions.
     */
    public void setLogFailuresOnly(boolean logFailuresOnly) {
        this.logFailuresOnly = logFailuresOnly;
    }


    @Override
    public void open(Configuration config) throws Exception {
        ConnectionFactory factory = rmqConnectionConfig.getConnectionFactory();
        try {
            connection = factory.newConnection();
            channel = connection.createChannel();
            if (channel == null) {
                throw new RuntimeException("None of RabbitMQ channels are available");
            }
            setupExchange();
        } catch (IOException e) {
            throw new RuntimeException("Error while creating the channel", e);
        }
    }

    /**
     * Called when new data arrives to the sink, and forwards it to RMQ.
     *
     * @param value
     *            The incoming data
     */
    @Override
    public void invoke(KLine value) {
        String code = value.getCode();
        try {
            byte[] msg = schema.serialize(JSON.toJSONString(value));
            channel.basicPublish(this.exchange, code, null, msg);
        } catch (IOException e) {
            if (logFailuresOnly) {
                LOG.error("Cannot send RMQ message {} at {}", code, rmqConnectionConfig.getHost(), e);
            } else {
                throw new RuntimeException("Cannot send RMQ message " + code +" at " + rmqConnectionConfig.getHost(), e);
            }
        }

    }

    @Override
    public void close() {
        IOException t = null;
        try {
            channel.close();
        } catch (IOException e) {
            t = e;
        }

        try {
            connection.close();
        } catch (IOException e) {
            if(t != null) {
                LOG.warn("Both channel and connection closing failed. Logging channel exception and failing with connection exception", t);
            }
            t = e;
        }
        if(t != null) {
            throw new RuntimeException("Error while closing RMQ connection with " + this.exchange
                    + " at " + rmqConnectionConfig.getHost(), t);
        }
    }

}
