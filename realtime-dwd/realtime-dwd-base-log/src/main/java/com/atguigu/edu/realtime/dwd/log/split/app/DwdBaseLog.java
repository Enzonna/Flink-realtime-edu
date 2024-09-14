package com.atguigu.edu.realtime.dwd.log.split.app;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.util.DateFormatUtil;
import com.atguigu.edu.realtime.common.util.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;

public class DwdBaseLog extends BaseApp {
    private final String ERR = "err";
    private final String START = "start";
    private final String DISPLAY = "display";
    private final String ACTION = "action";
    private final String PAGE = "page";
    private final String VIDEO = "video";

    public static void main(String[] args) {
        new DwdBaseLog().start(10012, 4, "dwd_base_log", "topic_log");
    }


    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // TODO 1. Perform type conversion on streaming data and perform simple ETL     jsonStr -> jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = etl(kafkaStrDS);
        // jsonObjDS.print("etl✅✅:");

        // TODO 2. Repairs 新老访客 label
        SingleOutputStreamOperator<JSONObject> fixedDS = fixedNewAndOld(jsonObjDS);
        // fixedDS.print("fixed:");

        // TODO 3. Splitting, putting different types of logs into different streams
        Map<String, DataStream> streamMap = splitStream(fixedDS);

//        // TODO 4. Write different data to the topic of Kafka
        writeToKafka(streamMap);
    }


    private void writeToKafka(Map<String, DataStream> streamMap) {
        streamMap
                .get(PAGE)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        streamMap
                .get(ERR)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        streamMap
                .get(START)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        streamMap
                .get(DISPLAY)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        streamMap
                .get(ACTION)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
        streamMap
                .get(VIDEO)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_VIDEO));
    }

    private Map<String, DataStream> splitStream(SingleOutputStreamOperator<JSONObject> fixedDS) {
        OutputTag<String> errTag = new OutputTag<String>("errTag") {
        };
        OutputTag<String> startTag = new OutputTag<String>("startTag") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {
        };
        OutputTag<String> actionTag = new OutputTag<String>("actionTag") {
        };
        OutputTag<String> appVideoTag = new OutputTag<String>("videoTag") {
        };
        SingleOutputStreamOperator<String> pageDS = fixedDS.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObj, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                        JSONObject errJsonObj = jsonObj.getJSONObject("err");
                        if (errJsonObj != null) {
                            ctx.output(errTag, jsonObj.toJSONString());
                            jsonObj.remove("err");
                        }

                        JSONObject startJsonObj = jsonObj.getJSONObject("start");
                        if (startJsonObj != null) {
                            ctx.output(startTag, jsonObj.toJSONString());
                        } else {
                            Long ts = jsonObj.getLong("ts");
                            JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                            JSONObject appVideo = jsonObj.getJSONObject("appVideo");
                            if (appVideo != null) {
                                ctx.output(appVideoTag, jsonObj.toJSONString());
                            } else {
                                JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                                JSONArray displaysArr = jsonObj.getJSONArray("displays");
                                if (displaysArr != null && displaysArr.size() > 0) {
                                    for (int i = 0; i < displaysArr.size(); i++) {
                                        JSONObject displayJsonObj = displaysArr.getJSONObject(i);
                                        JSONObject newDisplayJsonObj = new JSONObject();
                                        newDisplayJsonObj.put("common", commonJsonObj);
                                        newDisplayJsonObj.put("page", pageJsonObj);
                                        newDisplayJsonObj.put("ts", ts);
                                        newDisplayJsonObj.put("display", displayJsonObj);
                                        ctx.output(displayTag, newDisplayJsonObj.toJSONString());
                                    }
                                    jsonObj.remove("displays");
                                }

                                JSONArray actionsArr = jsonObj.getJSONArray("actions");
                                if (actionsArr != null && actionsArr.size() > 0) {
                                    for (int i = 0; i < actionsArr.size(); i++) {
                                        JSONObject actionJsonObj = actionsArr.getJSONObject(i);
                                        JSONObject newActionJsonObj = new JSONObject();
                                        newActionJsonObj.put("common", commonJsonObj);
                                        newActionJsonObj.put("page", pageJsonObj);
                                        newActionJsonObj.put("action", actionJsonObj);
                                        ctx.output(actionTag, newActionJsonObj.toJSONString());
                                    }
                                    jsonObj.remove("actions");
                                }

                                out.collect(jsonObj.toJSONString());
                            }

                        }
                    }
                }
        );
        SideOutputDataStream<String> errDS = pageDS.getSideOutput(errTag);
        SideOutputDataStream<String> startDS = pageDS.getSideOutput(startTag);
        SideOutputDataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        SideOutputDataStream<String> actionDS = pageDS.getSideOutput(actionTag);
        SideOutputDataStream<String> videoDS = pageDS.getSideOutput(appVideoTag);



        errDS.print("err--:✅");
        pageDS.print("page--:✅");
        startDS.print("start--:✅");
        displayDS.print("display--:✅");
        actionDS.print("action--:✅");
        videoDS.print("video--:✅");

        Map<String, DataStream> splitMap = new HashMap<>();
        splitMap.put("err", errDS);
        splitMap.put("start", startDS);
        splitMap.put("display", displayDS);
        splitMap.put("action", actionDS);
        splitMap.put("page", pageDS);
        splitMap.put("video", videoDS);


        return splitMap;
    }

    private static SingleOutputStreamOperator<JSONObject> fixedNewAndOld(SingleOutputStreamOperator<JSONObject> jsonObjDS) {
        KeyedStream<JSONObject, String> keyedDS =
                jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> fixedDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    private ValueState<String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor =
                                new ValueStateDescriptor<>("lastVisitDate", String.class);
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        String isNew = jsonObj.getJSONObject("common").getString("is_new");
                        String lastVisitDate = lastVisitDateState.value();
                        Long ts = jsonObj.getLong("ts");
                        String curVisitDate = DateFormatUtil.tsToDate(ts);

                        if ("1".equals(isNew)) {
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                lastVisitDateState.update(curVisitDate);
                            } else {
                                if (!lastVisitDate.equals(curVisitDate)) {
                                    isNew = "0";
                                    jsonObj.getJSONObject("common").put("is_new", isNew);
                                }
                            }

                        } else {
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                long yesterDayTs = ts - 24 * 60 * 60 * 1000;
                                String yesterDay = DateFormatUtil.tsToDate(yesterDayTs);
                                lastVisitDateState.update(yesterDay);
                            }
                        }
                        out.collect(jsonObj);
                    }
                }
        );
        return fixedDS;
    }

    private static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaStrDS) {
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag") {
        };

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            out.collect(jsonObj);
                        } catch (Exception e) {
                            ctx.output(dirtyTag, jsonStr);
                        }
                    }
                }
        );

        SideOutputDataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
        KafkaSink<String> kafkaSink = FlinkSinkUtil.getKafkaSink("dirty_data");
        dirtyDS.sinkTo(kafkaSink);
        return jsonObjDS;
    }
}
