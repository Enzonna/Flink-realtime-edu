package com.atguigu.gmall.servicce;

import com.atguigu.gmall.bean.TrafficUvCt;
import org.springframework.stereotype.Service;

import java.util.List;


public interface TrafficSourceStatsService {
    
    // 1. 获取各来源独立访客数
    List<TrafficUvCt> selectMid(Integer date);

}