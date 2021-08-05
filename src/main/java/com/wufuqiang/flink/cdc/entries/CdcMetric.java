package com.wufuqiang.flink.cdc.entries;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Wu Fuqiang
 * @date 2021/8/4 5:31 下午
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CdcMetric {
    private String name;
    private int count;
    private long size;
    private long delay;
}
