package com.wufuqiang.flink.cdc.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Wu Fuqiang
 * @date 2021/7/29 2:11 下午
 */

public class MySqlBinlogSourceTableExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings tableEnvSettings = EnvironmentSettings.newInstance().useBlinkPlanner()
                .inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, tableEnvSettings);
        String sql = "create table mysql_binlog";
    }
}
