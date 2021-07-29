package com.wufuqiang.flink.cdc;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import com.wufuqiang.flink.cdc.deserializer.CanalDeserializationSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.formats.json.canal.CanalJsonDeserializationSchema;
import org.apache.flink.formats.json.canal.CanalJsonFormatFactory;
import org.apache.flink.formats.json.canal.CanalJsonOptions;
import org.apache.flink.formats.json.canal.CanalJsonSerializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
        ParameterTool config = ParameterTool.fromPropertiesFile(configPath);
        String host = config.get("test.host");
        int port = Integer.parseInt(config.get("test.port"));
        String[] databaseList = config.get("test.database.list").split(",");
        String[] tableList = config.get("test.table.list").split(",");
        String username = config.get("test.username");
        String password = config.get("test.password");
        String checkpointDir = config.get("test.checkpoint.dir");
        System.out.println(host);

        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname(host).port(port).username(username).password(password)
                .databaseList(databaseList)
                .tableList(tableList)
                .startupOptions(StartupOptions.latest())
                .deserializer(new CanalDeserializationSchema())
//                .deserializer(new StringDebeziumDeserializationSchema())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000*5);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,2000));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new FsStateBackend(checkpointDir));

        env.addSource(sourceFunction).returns(String.class).print().setParallelism(1);

        env.execute("MySqlBinlogSourceExample");
    }
}
