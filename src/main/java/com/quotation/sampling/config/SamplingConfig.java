package com.quotation.sampling.config;

import lombok.Data;

/**
 * @Classname SamplingConfig
 * @Description TODO
 * @Date 2020/5/11 下午2:26
 * @Created by zqw
 * @Version 1.0
 */
@Data
public class SamplingConfig {
    //common setting
    private boolean isCheckTradingTime;
    private String db;
    private String mq;

    //db
    private String dbName;
    private String dbHost;
    private Integer dbPort;
    //mq
    private String mqHost;
    private Integer mqPort;
    private String mqUserName;
    private String mqPassword;
    private String exchange;


}
