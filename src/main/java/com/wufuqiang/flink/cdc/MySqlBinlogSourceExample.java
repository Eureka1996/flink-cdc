package com.wufuqiang.flink.cdc;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.wufuqiang.flink.cdc.deserializer.CanalDeserializationSchema;
import com.wufuqiang.flink.cdc.partitioner.MyKafkaPartitioner;
import com.wufuqiang.flink.cdc.partitioner.MyTestKafkaPartitioner;
import com.wufuqiang.flink.cdc.schema.MySerializationSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

import java.util.Optional;
import java.util.Properties;

/**
 * @author Wu Fuqiang
 * @date 2021/7/23 11:12 上午
 */

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


        env.execute("MySqlBinlogSourceExample");
    }

    public static FlinkKafkaProducer createMyKafkaProducer(String servers,String topic){
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers",servers);
        Optional<FlinkKafkaPartitioner> ps = Optional.of(new MyKafkaPartitioner());
        Optional<FlinkKafkaPartitioner> ps2 = Optional.of(new MyTestKafkaPartitioner());
        return new FlinkKafkaProducer(
                topic,
                new MySerializationSchema(topic),    // serialization schema
                kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

    }
}
