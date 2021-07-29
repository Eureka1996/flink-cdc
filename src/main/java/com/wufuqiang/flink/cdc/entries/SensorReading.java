package com.wufuqiang.flink.cdc.entries;

/**
 * @author Wu Fuqiang
 * @date 2021/7/27 3:58 下午
 */

public class SensorReading {
    private String id;
    private Long timestamp;
    private Double temperature;


    public SensorReading() {
    }

    public SensorReading(String id, Long timestamp, Double temperature) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "温度探测器：" +
                "\n\tid=" + id +
                "\n\ttimestamp=" + timestamp +
                "\n\ttemperature=" + temperature
                ;
    }
}
