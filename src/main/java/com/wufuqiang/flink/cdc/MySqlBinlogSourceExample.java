package com.wufuqiang.flink.cdc;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.wufuqiang.flink.cdc.deserializer.CanalDeserializationSchema;
import com.wufuqiang.flink.cdc.entries.CdcMetric;
import com.wufuqiang.flink.cdc.partitioner.MyKafkaPartitioner;
import com.wufuqiang.flink.cdc.schema.JSONObjectSerializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Optional;
import java.util.Properties;

/**
 * @author Wu Fuqiang
 * @date 2021/7/23 11:12 上午
 */
@Slf4j
public class MySqlBinlogSourceExample {
    public static void main(String[] args) throws Exception {
        String configPath = "db.properties";
//        if(args != null){
//            ParameterTool parameters = ParameterTool.fromArgs(args);
//            configPath = parameters.get("config_path");
//        }
        // 获取配置信息
        ParameterTool config = ParameterTool.fromPropertiesFile(configPath);
        String host = config.get("test.host");
        int port = Integer.parseInt(config.get("test.port"));
        String[] databaseList = config.get("test.database.list").split(",");
        String[] tableList = config.get("test.table.list").split(",");
        String username = config.get("test.username");
        String password = config.get("test.password");
        String checkpointDir = config.get("test.checkpoint.dir");
        log.info("$￥￥￥￥￥￥￥￥￥￥￥￥￥￥￥￥￥￥￥￥￥￥host:{},port:{},username:{}",host,port,username);

        // 获取拉取binlog的数据源
        DebeziumSourceFunction<JSONObject> sourceFunction = MySQLSource.<JSONObject>builder()
                .hostname(host).port(port).username(username).password(password)
                .databaseList(databaseList).tableList(tableList)
                .startupOptions(StartupOptions.latest())
                .deserializer(new CanalDeserializationSchema())
//                .deserializer(new StringDebeziumDeserializationSchema())
                .build();

        // 配置checkpoint
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000*5);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,2000));
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new FsStateBackend(checkpointDir));

        String servers = config.get("test.kafka.bootstrap.servers");
        String topic = config.get("test.kafka.topic");
        FlinkKafkaProducer myKafkaProducer = createMyKafkaProducer(servers, topic);
        env.addSource(sourceFunction).addSink(myKafkaProducer);

        env.addSource(sourceFunction)
                .map(item->{
                    String key = item.getString("database");
                    long size = item.toJSONString().getBytes().length;
                    long delay =System.currentTimeMillis() - item.getLong("es");
                    return new CdcMetric(key,1,size,delay);
                })
                .returns(TypeInformation.of(CdcMetric.class))
                .keyBy(item -> item.getName())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .reduce((item1,item2)->
                        new CdcMetric(item1.getName(),item1.getCount()+item2.getCount(),
                                item1.getSize()+item2.getSize(),item1.getDelay()+item2.getDelay())
                        )
                .map(item ->new Tuple4<>(item.getName(),
                        item.getCount(),
                        getSizeDescription(item.getSize()),
                        item.getDelay()*1.0/item.getCount()))
                .returns(TypeInformation.of(new TypeHint<Tuple4<String, Integer, String,Double>>() {}))
                .print()
        ;
        env.execute("MySqlBinlogSourceExample");
    }

    public static FlinkKafkaProducer createMyKafkaProducer(String servers,String topic){
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers",servers);
        kafkaProperties.setProperty("transaction.timeout.ms",String.valueOf(1000*60*5));
        kafkaProperties.setProperty("flink.partition-discovery.interval-millis", "50000");
        Optional<FlinkKafkaPartitioner> ps = Optional.of(new MyKafkaPartitioner());
        // 此种方式没办法按key进行分区
//        return new FlinkKafkaProducer(
//                topic,
//                new MySerializationSchema(topic),    // serialization schema
//                kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        return new FlinkKafkaProducer(topic,new JSONObjectSerializationSchema(),kafkaProperties,ps);

    }

    public static String getSizeDescription(long size) {
        StringBuffer bytes = new StringBuffer();
        DecimalFormat format = new DecimalFormat("###.0");
        if (size >= 1024 * 1024 * 1024) {
            double i = (size / (1024.0 * 1024.0 * 1024.0));
            bytes.append(format.format(i)).append("GB");
        }
        else if (size >= 1024 * 1024) {
            double i = (size / (1024.0 * 1024.0));
            bytes.append(format.format(i)).append("MB");
        }
        else if (size >= 1024) {
            double i = (size / (1024.0));
            bytes.append(format.format(i)).append("KB");
        }
        else if (size < 1024) {
            if (size <= 0) {
                bytes.append("0B");
            }
            else {
                bytes.append((int) size).append("B");
            }
        }
        return bytes.toString();
    }
}
