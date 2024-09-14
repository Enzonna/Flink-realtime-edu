package com.atguigu.edu.realtime.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DwsTrafficPageViewWindowBean {

    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;


    String cur_date;

    // 设备 ID
    String mid;

    // 页面 ID
    String pageId;

    // 首页独立访客数
    Long homeUvCount;

    // 课程列表页独立访客数
    Long listUvCount;

    // 课程详情页独立访客数
    Long detailUvCount;

    @JSONField(serialize = false)
    // 时间戳
    Long ts;
}