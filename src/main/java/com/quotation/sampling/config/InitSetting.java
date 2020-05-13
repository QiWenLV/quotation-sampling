package com.quotation.sampling.config;

import cn.hutool.setting.Setting;
import com.quotation.sampling.utils.MongoManager;

import java.util.Objects;
import java.util.Optional;

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

    public static SamplingConfig getSettingByGroup(String groupName) {
        SamplingConfig samplingConfig = new SamplingConfig();
        Setting setting = new Setting("start.setting");
        setting.toBean(groupName, samplingConfig);
        setting.toBean(samplingConfig.getDb(), samplingConfig);
        setting.toBean(samplingConfig.getMq(), samplingConfig);
        return samplingConfig;
    }

    public static SamplingConfig initSetting(String...args){
        String groupName = "dev";
        if(Objects.nonNull(args) && args.length >= 1){
            groupName = args[0];
        }
        SamplingConfig samplingConfig =  getSettingByGroup(groupName);

        MongoManager.initDBPrompties(samplingConfig.getDbHost(), samplingConfig.getDbPort());
        return samplingConfig;
    }
}
