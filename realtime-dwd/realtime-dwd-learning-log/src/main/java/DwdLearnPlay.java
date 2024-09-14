import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.bean.DwdLearnPlayBean;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwdLearnPlay extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwdLearnPlay().start(10015,
                4,
                "dwd_traffic_user_jump_detail",
                "dwd_traffic_video");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource) {
        // DataStream<String> rebalanceDS = kafkaSource.rebalance();

        SingleOutputStreamOperator<DwdLearnPlayBean> mappedStream = kafkaSource.map(
                jsonStr -> {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    JSONObject common = jsonObj.getJSONObject("common");
                    JSONObject appVideo = jsonObj.getJSONObject("appVideo");
                    Long ts = jsonObj.getLong("ts");
                    return DwdLearnPlayBean.builder()
                            .sourceId(common.getString("sc"))
                            .provinceId(common.getString("ar"))
                            .userId(common.getString("uid"))
                            .operatingSystem(common.getString("os"))
                            .channel(common.getString("ch"))
                            .isNew(common.getString("is_new"))
                            .model(common.getString("md"))
                            .machineId(common.getString("mid"))
                            .versionCode(common.getString("vc"))
                            .brand(common.getString("ba"))
                            .sessionId(common.getString("sid"))
                            .playSec(appVideo.getInteger("play_sec"))
                            .positionSec(appVideo.getInteger("position_sec"))
                            .videoId(appVideo.getString("video_id"))
                            .ts(ts)
                            .build();
                }
        );
        //mappedStream.print("mappedStream--:✅");

        SingleOutputStreamOperator<DwdLearnPlayBean> withWatermarkStream = mappedStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwdLearnPlayBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<DwdLearnPlayBean>() {
                                    @Override
                                    public long extractTimestamp(DwdLearnPlayBean dwdLearnPlayBean, long recordTimestamp) {
                                        return dwdLearnPlayBean.getTs();
                                    }
                                }
                        )
        );

        KeyedStream<DwdLearnPlayBean, String> keyedStream =
                withWatermarkStream.keyBy(DwdLearnPlayBean::getSessionId);
        //keyedStream.print("keyedStream--:✅");

        WindowedStream<DwdLearnPlayBean, String, TimeWindow> windowedStream =
                keyedStream.window(EventTimeSessionWindows.withGap(Time.seconds(3L)));




        SingleOutputStreamOperator<DwdLearnPlayBean> reducedStream = windowedStream.reduce(
                new ReduceFunction<DwdLearnPlayBean>() {
                    @Override
                    public DwdLearnPlayBean reduce(DwdLearnPlayBean value1, DwdLearnPlayBean value2) throws Exception {
                        Integer playSec1 = value1.getPlaySec();
                        Integer playSec2 = value2.getPlaySec();
                        Long ts1 = value1.getTs();
                        Long ts2 = value2.getTs();
                        value1.setPlaySec(playSec1 + playSec2);
                        if (ts2 > ts1) {
                            value1.setPositionSec(value2.getPositionSec());
                        }

                        return value1;
                    }
                },
                new ProcessWindowFunction<DwdLearnPlayBean, DwdLearnPlayBean, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<DwdLearnPlayBean> elements, Collector<DwdLearnPlayBean> out) throws Exception {
                        System.out.println("elements = " + elements);
                        for (DwdLearnPlayBean element : elements) {
                            System.out.println("element = " + element);
                            out.collect(element);
                        }
                    }
                }
        );
        reducedStream.print("reducedStream--:✅");

        SingleOutputStreamOperator<String> jsonStrStream = reducedStream.map(JSON::toJSONString);
        // jsonStrStream.print();
    }
}
