package com.wufuqiang.flink.cdc.schema;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * @author: Wu Fuqiang
 * @create: 2021-07-31 00:37
 */
public class MySerializationSchema implements KafkaSerializationSchema<JSONObject> {
    private String topic;

    public MySerializationSchema(String topic) {
        this.topic = topic;
    }


    @Override
    public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
        String key = element.getString("key");
        if(key == null) key = "";
        System.out.println(key.hashCode()%3);
        return new ProducerRecord<>(this.topic,key.getBytes(),element.toJSONString().getBytes());
    }
}
