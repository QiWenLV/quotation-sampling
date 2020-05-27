package com.quotation.sampling.config;

import cn.hutool.setting.Setting;

import java.util.Objects;

/**
 * @Classname InitSetting
 * @Description TODO
 * @Date 2020/5/11 下午2:21
 * @Created by zqw
 * @Version 1.0
 */
public class InitSetting {

//    public InitSetting() {
//        this("demo");
//    }
    public static SamplingConfig samplingConfig;

    private InitSetting(){}

    /**
     * 根据配置分组名获取配置信息
     * @param groupName
     * @return
     */
    public static SamplingConfig getSettingByGroup(String groupName) {
        SamplingConfig samplingConfig = new SamplingConfig();
        Setting setting = new Setting("start.setting");
        setting.toBean(groupName, samplingConfig);
        setting.toBean(samplingConfig.getDb(), samplingConfig);
        setting.toBean(samplingConfig.getMq(), samplingConfig);
        return samplingConfig;
    }

    /**
     * 初始化配置信息
     * @param args
     * @return
     */
    public static SamplingConfig initSetting(String...args){
        String groupName = "dev";
        if(Objects.nonNull(args) && args.length >= 1){
            groupName = args[0];
        }
        InitSetting.samplingConfig =  getSettingByGroup(groupName);

//        MongoManager.initMongoDB(samplingConfig.getDbHost(), samplingConfig.getDbPort());
        return InitSetting.samplingConfig;
    }
}
