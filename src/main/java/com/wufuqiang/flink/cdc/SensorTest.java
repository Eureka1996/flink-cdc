package com.wufuqiang.flink.cdc;

import com.wufuqiang.flink.cdc.source.SensorSource;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Wu Fuqiang
 * @date 2021/7/27 4:10 下午
 */

public class SensorTest {
    public static void main(String[] args) throws Exception {



        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000*5);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,2000));
        env.setStateBackend(new FsStateBackend("hdfs:///tools/flink-checkpoints"));



        env.addSource(new SensorSource()).print().setParallelism(1);

        env.execute("Senso");
    }
}
