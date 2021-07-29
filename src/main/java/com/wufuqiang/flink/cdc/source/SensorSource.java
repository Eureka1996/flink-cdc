package com.wufuqiang.flink.cdc.source;

import com.wufuqiang.flink.cdc.entries.SensorReading;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.Serializable;

/**
 * @author Wu Fuqiang
 * @date 2021/7/27 3:57 下午
 */

public class SensorSource implements SourceFunction<SensorReading>, Serializable {

    private Boolean running = true;

    @Override
    public void run(SourceContext<SensorReading> sourceContext) throws Exception {
        while(running){
            SensorReading sensor = new SensorReading("sensor_wufuqiang", System.currentTimeMillis(), 18.56);
            sourceContext.collect(sensor);
            Thread.sleep(10000);
        }
    }
    
    @Override
    public void cancel() {
        running = false;
    }
}
