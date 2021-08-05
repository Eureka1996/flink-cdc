package com.wufuqiang.flink.cdc.schema;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SerializationSchema;

/**
 * @author Wu Fuqiang
 * @date 2021/8/2 2:26 下午
 */

public class JSONObjectSerializationSchema implements SerializationSchema<JSONObject> {
    @Override
    public byte[] serialize(JSONObject jsonObject) {
        return jsonObject.toJSONString().getBytes();
    }
}
