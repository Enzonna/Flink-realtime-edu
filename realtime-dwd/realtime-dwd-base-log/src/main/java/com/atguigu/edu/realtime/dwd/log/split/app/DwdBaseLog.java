package com.atguigu.edu.realtime.dwd.log.split.app;

import com.enzo.gmall.realtime.common.base.BaseApp;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DwdBaseLog extends BaseApp {
    public static void main(String[] args) {
        new DwdBaseLog().start(10011, 4, "dwd_base_log", "topic_log");
    }


    @Override
    public void handle(StreamExecutionEnvironment streamExecutionEnvironment, DataStreamSource<String> dataStreamSource) {

    }
}
