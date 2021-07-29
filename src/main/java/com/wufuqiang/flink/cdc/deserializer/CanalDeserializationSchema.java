package com.wufuqiang.flink.cdc.deserializer;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * @author Wu Fuqiang
 * @date 2021/7/28 4:31 下午
 */

public class CanalDeserializationSchema implements DebeziumDeserializationSchema<String> {

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        JSONObject result = new JSONObject();

        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        Struct value = (Struct)sourceRecord.value();
        /**
         * INSERT语句：before为null,after为新增的记录值
         * UPDATE语句：before为更新前的数据，after为更新后的数据
         * DELETE语句：before为删除前数据，after为null
         */
        Struct before = value.getStruct("before");
        Struct after = value.getStruct("after");
        Struct source = value.getStruct("source");
        if(before != null){
            JSONObject data = new JSONObject();
            for (Field field : before.schema().fields()) {
                Object o = before.get(field);
                data.put(field.name(), o);
            }
            JSONArray jsonArray = new JSONArray();
            jsonArray.add(data);
            result.put("old",jsonArray);
        }
        if(after!=null){
            JSONObject data = new JSONObject();
            for (Field field : after.schema().fields()) {
                Object o = after.get(field);
                data.put(field.name(), o);
            }
            JSONArray jsonArray = new JSONArray();
            jsonArray.add(data);
            result.put("data",jsonArray);
        }
        result.put("database", source.getString("db"));
        //事件时间
        result.put("es",source.getInt64("ts_ms"));
        //摄入时间
        result.put("ts",value.getInt64("ts_ms"));
        result.put("table", source.getString("table"));
        String type = operation.toString().toLowerCase();
        if("create".equals(type)) type = "insert";
        result.put("type", type);
        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return null;
    }
}
