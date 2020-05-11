package com.quotation.sampling.config;

import cn.hutool.setting.Setting;

/**
 * @Classname InitSetting
 * @Description TODO
 * @Date 2020/5/11 下午2:21
 * @Created by zqw
 * @Version 1.0
 */
public class InitSetting {

    public InitSetting() {
        this("demo");
    }

    public InitSetting(String groupName) {
        Setting setting = new Setting("XXX.setting");
        setting.getBool("isCheckTradingTime", groupName, true);
    }
}
