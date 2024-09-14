package com.atguigu.gmall.mapper;

import com.atguigu.gmall.bean.TrafficUvCt;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface TrafficSourceStatsMapper {

    // 1. 获取各来源独立访客数
    @Select("select\n" +
            "    mid,\n" +
            "    sum(home_uv_count)   home_uv_count,\n" +
            "    sum(list_uv_count)   list_uv_count,\n" +
            "    sum(detail_uv_count) detail_uv_count\n" +
            "from\n" +
            "    dws_traffic_source_keyword_page_view_window\n" +
            "partition par20240913\n" +
            "group by mid")
    List<TrafficUvCt> selectMid(Integer date);

}