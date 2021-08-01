package com.wufuqiang.flink.cdc.partitioner;

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

/**
 * @author: Wu Fuqiang
 * @create: 2021-07-30 23:16
 */
public class MyTestKafkaPartitioner extends FlinkKafkaPartitioner<String> {

    @Override
    public int partition(String record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        return 0;
    }
}
