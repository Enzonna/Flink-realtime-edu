package com.atguigu.edu.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

public class DwdTrafficUserJumpDetail extends BaseApp {
    public static void main(String[] args) {
        new DwdTrafficUserJumpDetail().start(10014, 4, "dwd_traffic_user_jump_detail", "dwd_traffic_page_log");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource) {

        // 测试数据
        DataStream<String> kafkaSource1 = env
            .fromElements(
                "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                    "\"home\"},\"ts\":15000} ",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                    "\"detail\"},\"ts\":30000} "
            );


        //过滤加转换数据
        SingleOutputStreamOperator<JSONObject> jsonObjStream = kafkaSource.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        // 添加水位线
        SingleOutputStreamOperator<JSONObject> withWatermarkStream = jsonObjStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                }));

        // 按照mid分组
        KeyedStream<JSONObject, String> keyedStream = withWatermarkStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("common").getString("mid");
            }
        });
        // keyedStream.print("🚀");


        // 定义cep匹配规则
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first").where(new IterativeCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject, Context<JSONObject> ctx) throws Exception {
                // 一个会话的开头   ->   last_page_id 为空
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                return lastPageId == null;
            }
        }).next("second").where(new IterativeCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject, Context<JSONObject> ctx) throws Exception {
                // 满足匹配的条件
                // 紧密相连  又一个会话的开头
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                return lastPageId == null;
            }
        }).within(Time.seconds(10L));


        // 将CEP作用到流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        // 提取匹配数据和超时数据
        OutputTag<String> timeoutTag = new OutputTag<String>("timeoutTag") {
        };

        SingleOutputStreamOperator<String> flatSelectStream = patternStream.flatSelect(timeoutTag, new PatternFlatTimeoutFunction<JSONObject, String>() {
            @Override
            public void timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp, Collector<String> out) throws Exception {
                System.out.println("pattern = " + pattern);
                JSONObject first = pattern.get("first").get(0);
                out.collect(first.toJSONString());
            }
        }, new PatternFlatSelectFunction<JSONObject, String>() {
            @Override
            public void flatSelect(Map<String, List<JSONObject>> pattern, Collector<String> out) throws Exception {
                System.out.println("flatSelect = " + pattern);
                JSONObject first = pattern.get("first").get(0);
                out.collect(first.toJSONString());
            }
        });

        SideOutputDataStream<String> timeoutStream = flatSelectStream.getSideOutput(timeoutTag);

        DataStream<String> unionStream = flatSelectStream.union(timeoutStream);
        timeoutStream.print("❌");
        unionStream.print("✅");

        unionStream.sinkTo(FlinkSinkUtil.getKafkaSink("dwd_traffic_unique_visitor_detail"));


    }
}
