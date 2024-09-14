package com.atguigu.gmall.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TrafficUvCt {

    // 来源
    String source_name;

    // 独立访客数
    Long uv_count;
}