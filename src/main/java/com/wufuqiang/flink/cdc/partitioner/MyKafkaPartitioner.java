package com.wufuqiang.flink.cdc.partitioner;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

import java.util.Arrays;

/**
 * @author: Wu Fuqiang
 * @create: 2021-07-30 16:23
 */
public class MyKafkaPartitioner extends FlinkKafkaPartitioner<JSONObject> {
    @Override
    public int partition(JSONObject jsonObject, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        String partitionKey = jsonObject.getString("key");
        int index = Math.abs(hash(partitionKey)%partitions.length);
//        System.out.println("kafka partition index:"+index+",partitions:"+partitions.length);
        return partitions[index];
    }

    static final int hash(Object key) {
        int h;
        return (key == null) ? 0 : (h = key.hashCode()) ^ (h >>> 16);
    }
}
