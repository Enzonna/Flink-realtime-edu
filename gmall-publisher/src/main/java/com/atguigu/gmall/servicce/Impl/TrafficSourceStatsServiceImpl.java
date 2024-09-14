package com.atguigu.gmall.servicce.Impl;

import com.atguigu.gmall.bean.TrafficUvCt;
import com.atguigu.gmall.mapper.TrafficSourceStatsMapper;
import com.atguigu.gmall.servicce.TrafficSourceStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TrafficSourceStatsServiceImpl implements TrafficSourceStatsService {
    // 自动装载 Mapper 接口实现类
    @Autowired
    TrafficSourceStatsMapper trafficSourceStatsMapper;

    // 1. 获取各来源独立访客数
    @Override
    public List<TrafficUvCt> selectMid(Integer date) {
        return trafficSourceStatsMapper.selectMid(date);
    }
}