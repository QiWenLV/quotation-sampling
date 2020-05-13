package com.quotation.sampling.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * @Classname KLine
 * @Description K线实体类
 * @Date 2020/5/9 下午1:04
 * @Created by zqw
 * @Version 1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class KLine {

    private LocalDateTime datetime;
    private LocalDateTime updatetime;
    private String code;
    private String exchange;
    private double open;
    private double high;
    private double low;
    private double close;
    private int volume;
    private int frequence;


    public void updateHigh(double lastPrice){
        this.high = Math.max(high, lastPrice);
    }

    public void updateLow(double lastPrice){
        this.low = Math.min(low, lastPrice);
    }

    public void updateClose(double lastPrice){
        this.close = lastPrice;
    }

    public void updateVolume(int volume){
        this.volume += volume;
    }
}
