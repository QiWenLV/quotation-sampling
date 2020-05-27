package com.quotation.sampling.operator;

import com.quotation.sampling.bean.KLine;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Classname SamplingProcessFunction
 * @Description TODO
 * @Date 2020/5/9 下午4:04
 * @Created by zqw
 * @Version 1.0
 */
public class SamplingProcessFunction extends ProcessWindowFunction<KLine, KLine, String, TimeWindow> {

    private Map<String, OutputTag<KLine>> OUTPUT_TAGS;

    public SamplingProcessFunction(Map<String, OutputTag<KLine>> OUTPUT_TAGS) {
        this.OUTPUT_TAGS = OUTPUT_TAGS;
    }

    @Override
    public void process(String key, Context context, Iterable<KLine> elements, Collector<KLine> out) throws Exception {
        LocalDateTime datetime = LocalDateTime.ofEpochSecond(context.window().getEnd() / 1000, 0, ZoneOffset.ofHours(8));
        String[] linkKey = key.split("_");
        KLine kLine = KLine.builder()
                .code(linkKey[1])
                .exchange(linkKey[0])
                .timestamp(LocalDateTime.now().toInstant(ZoneOffset.of("+8")).toEpochMilli())
                .datetime(datetime)
                .frequence((int) (context.window().getEnd() - context.window().getStart()) / 60000)
                .build();
        AtomicBoolean firstFlag = new AtomicBoolean(true);
        elements.forEach(x -> {
            double lastPrice = x.getClose();
            if (firstFlag.get()) {
                kLine.setOpen(lastPrice);
                kLine.setHigh(lastPrice);
                kLine.setLow(lastPrice);
                kLine.setClose(lastPrice);
                kLine.setVolume(x.getVolume());
                kLine.setAmount(lastPrice * x.getVolume());
                firstFlag.set(false);
            } else {
                kLine.updateHigh(lastPrice);
                kLine.updateLow(lastPrice);
                kLine.updateVolume(x.getVolume());
                kLine.updateClose(lastPrice);
                kLine.updateAmount(lastPrice * x.getVolume());
            }
        });
        context.output(OUTPUT_TAGS.get("min" + kLine.getFrequence()), kLine);
        if ((kLine.getFrequence() == 5)) {
            out.collect(kLine);
        }
    }
}
