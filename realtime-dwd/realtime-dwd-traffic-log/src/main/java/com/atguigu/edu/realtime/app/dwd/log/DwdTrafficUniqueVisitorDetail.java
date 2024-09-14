package com.atguigu.edu.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.util.DateFormatUtil;
import com.atguigu.edu.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DwdTrafficUniqueVisitorDetail extends BaseApp {
    public static void main(String[] args) {
        new DwdTrafficUniqueVisitorDetail().start(10013, 4, "dwd_traffic_user_jump_detail", "dwd_traffic_page_log");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource) {
        SingleOutputStreamOperator<JSONObject> beanDS = kafkaSource
                .map(JSON::parseObject);

        SingleOutputStreamOperator<JSONObject> rsDS = beanDS.filter(json -> json.getJSONObject("page").getString("last_page_id") == null)
                .keyBy(json -> json.getJSONObject("common").getString("mid"))
                .filter(
                        new RichFilterFunction<JSONObject>() {
                            private ValueState<String> lastVisitDateState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<String>("last_visit", String.class);
                                valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1L)).build());
                                lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                            }

                            @Override
                            public boolean filter(JSONObject jsonObject) throws Exception {
                                String ts = DateFormatUtil.tsToDate(jsonObject.getLong("ts"));
                                String lastVisitDate = lastVisitDateState.value();
                                if (lastVisitDate == null || DateFormatUtil.toTs(lastVisitDate) < DateFormatUtil.toTs(ts)) {
                                    lastVisitDateState.update(ts);
                                    return true;
                                } else {
                                    return !lastVisitDate.equals(ts);
                                }
                            }
                        }
                );
        // rsDS.print();

        rsDS
                .map(JSONAware::toJSONString)
                .sinkTo(FlinkSinkUtil.getKafkaSink("dwd_traffic_unique_visitor_detail"));


    }
}
