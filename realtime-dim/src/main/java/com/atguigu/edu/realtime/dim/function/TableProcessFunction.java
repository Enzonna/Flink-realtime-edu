package com.atguigu.edu.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.bean.TableProcessDim;
import com.atguigu.edu.realtime.common.util.JdbcUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.*;

/**
 * Processing of linked data
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>> {
    // 🍵🍵🍵
    private Map<String, TableProcessDim> configMap = new HashMap<>();

    private MapStateDescriptor<String, TableProcessDim> mapStateDescriptor;

    public TableProcessFunction(MapStateDescriptor<String, TableProcessDim> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        // Preload the configuration information from the configuration into the program
        // 🍵🍵🍵
        Connection conn = JdbcUtil.getMySqlConnection();
        String sql = "select * from edu_config.table_process";
        List<TableProcessDim> tableProcessDimList = JdbcUtil.queryList(conn, sql, TableProcessDim.class, true);

        for (TableProcessDim tableProcessDim : tableProcessDimList) {
            configMap.put(tableProcessDim.getSourceTable(), tableProcessDim);
        }

        JdbcUtil.closeMySqlConnection(conn);
    }

    // processElement: 处理主流业务数据             根据广播状态中的配置信息判断当前处理的数据是不是维度信息
    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
        // 获取业务数据库的表名
        String key = jsonObject.getString("table");
        // 获取广播状态
        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        // 获取表名到广播状态中的配置信息
        TableProcessDim tableProcessDim = null;
        if ((tableProcessDim = broadcastState.get(key)) != null
                || (tableProcessDim = configMap.get(key)) != null) {
            // 如果从广播状态中获取的配置信息不为空，说明处理的是维度信息，将其中data部分以及对应的配置封装成二元组，发送到下游
            JSONObject dataJsonObj = jsonObject.getJSONObject("data");

            // 在向下游传递数据前，过滤掉不需要传递的属性
            String sinkColumns = tableProcessDim.getSinkColumns();
            deleteNotNeedColumn(dataJsonObj, sinkColumns);

            // 在向下游传递数据前，要添加type属性
            String type = jsonObject.getString("type");
            dataJsonObj.put("type", type);
            out.collect(Tuple2.of(dataJsonObj, tableProcessDim));
        }
    }

    // processBroadcastElement: 处理广播流数据      将配置信息放到广播状态中
    @Override
    public void processBroadcastElement(TableProcessDim tableProcessDim, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
        // 获取广播状态
        BroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        // 获取对配置表操作的类型
        String op = tableProcessDim.getOp();
        String key = tableProcessDim.getSourceTable();
        if ("d".equals(op)) {
            // 从配置表中删除了一条配置信息   从广播状态中删除对应的配置
            broadcastState.remove(key);
            configMap.remove(key);
        } else {
            // 从配置表中读取一条数据或者向配置表中添加了一条配置信息      将最新的配置更新到广播状态中
            broadcastState.put(key, tableProcessDim);
            configMap.put(key, tableProcessDim);
        }
    }


    /*
     * 过滤掉不需要传递的属性
     */
    private void deleteNotNeedColumn(JSONObject dataJsonObj, String sinkColumns) {
        List<String> columnList = Arrays.asList(sinkColumns.split(","));
        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();
        // removeIf 删除集合中满足条件的元素
        entrySet.removeIf(entry -> !columnList.contains(entry.getKey()));
    }

}
