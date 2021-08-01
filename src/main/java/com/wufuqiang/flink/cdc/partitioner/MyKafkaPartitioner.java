package com.wufuqiang.flink.cdc.partitioner;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

/**
 * @author: Wu Fuqiang
 * @create: 2021-07-30 16:23
 */
public class MyKafkaPartitioner extends FlinkKafkaPartitioner<JSONObject> {
    @Override
    public int partition(JSONObject jsonObject, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        String partitionKey = jsonObject.getString("key");
        int index = 0;
        if(partitionKey == null){
            index = partitionKey.hashCode() % partitions.length;
        }
        return partitions[index];
    }
}
