package com.atguigu.edu.realtime.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.bean.TableProcessDim;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.util.FlinkSourceUtil;
import com.atguigu.edu.realtime.common.util.HBaseUtil;
import com.atguigu.edu.realtime.dim.function.DimSinkFunction;
import com.atguigu.edu.realtime.dim.function.TableProcessFunction;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;


public class DimApp extends BaseApp {
    public static void main(String[] args) throws Exception {

        //🍵🍵🍵
        new DimApp().start(10002, 4, "dim_app", "topic_db");

    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {

        // TODO 1. 对流中数据进行类型转换并进行简单清洗ETL  jsonStr -> jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = etl(kafkaStrDS);

        // TODO 2. 使用FlinkCDC读取配置表数据
        SingleOutputStreamOperator<TableProcessDim> tpDS = readTableProcess(env);
        //tpDS.print();

        // TODO 3. 根据配置表中的信息到HBase中建标或者删表
        tpDS = createHBaseTable(tpDS);
        // tpDS.print();

        // TODO 4. 将配置流进行广播 --  broadcast,将主流和广播流进行关联  -- connect,将关联后的数据进行处理 --  process
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connect(jsonObjDS, tpDS);
        // dimDS.print();

        // TODO 5. 将维度数据写入HBase
        writeToHBase(dimDS);
    }


    private void writeToHBase(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS) {
        dimDS.print();
        dimDS.addSink(new DimSinkFunction());
    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connect(SingleOutputStreamOperator<JSONObject> jsonObjDS, SingleOutputStreamOperator<TableProcessDim> tpDS) {
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor =
                new MapStateDescriptor<>("mapStateDescriptor", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = tpDS.broadcast(mapStateDescriptor);

        // TODO 9. 将主流和广播流进行关联  -- connect
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(broadcastDS);


        // TODO 10. 将关联后的数据进行处理 --  process
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connectDS.process(
                new TableProcessFunction(mapStateDescriptor)
        );
        return dimDS;
    }

    private SingleOutputStreamOperator<TableProcessDim> createHBaseTable(SingleOutputStreamOperator<TableProcessDim> tpDS) {
        tpDS = tpDS.map(
                new RichMapFunction<TableProcessDim, TableProcessDim>() {
                    private Connection hBaseConnection;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hBaseConnection = HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hBaseConnection);
                    }

                    @Override
                    public TableProcessDim map(TableProcessDim tableProcessDim) throws Exception {
                        // 获取对配置表操作
                        String op = tableProcessDim.getOp();

                        String sinkTable = tableProcessDim.getSinkTable();
                        String[] families = tableProcessDim.getSinkFamily().split(",");

                        if ("c".equals(op) || "r".equals(op)) {
                            // 从配置表中读取或者向配置表中新增数据，在Hbase中执行建表操作
                            HBaseUtil.createHBaseTable(hBaseConnection, Constant.HBASE_NAMESPACE, sinkTable, families);

                        } else if ("d".equals(op)) {
                            // 从配置表中删除了一条配置信息，在Hbase中执行删表操作
                            HBaseUtil.dropHBaseTable(hBaseConnection, Constant.HBASE_NAMESPACE, sinkTable);

                        } else {
                            // 对配置表的信息进行了更新操作，先从Hbase中删除对应的表，再新建表
                            HBaseUtil.dropHBaseTable(hBaseConnection, Constant.HBASE_NAMESPACE, sinkTable);
                            HBaseUtil.createHBaseTable(hBaseConnection, Constant.HBASE_NAMESPACE, sinkTable, families);

                        }
                        return tableProcessDim;
                    }
                }
        ).setParallelism(1);
        return tpDS;
    }

    private SingleOutputStreamOperator<TableProcessDim> readTableProcess(StreamExecutionEnvironment env) {
        // TODO 5. 使用FlinkCDC从MySQL中读取配置信息
        // 5.1 创建MysqlSource
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource("edu_config", "table_process");

        // 5.2 读取数据，封装成流
        DataStreamSource<String> mysqlStrDS =
                env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source").setParallelism(1);
        // mysqlStrDS.print();

        // TODO 6. 对配置数据进行转换    jsonStr ->  实体类对象
        SingleOutputStreamOperator<TableProcessDim> tpDS = mysqlStrDS.map(
                new MapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String jsonStr) throws Exception {
                        // 将json字符串转换为jsonObj
                        JSONObject jsonObj = JSONObject.parseObject(jsonStr);
                        // 获取对配置表进行的操作的类型
                        String op = jsonObj.getString("op");
                        TableProcessDim tableProcessDim = null;
                        if ("d".equals(op)) {
                            // 说明从配置表中删除了一条数据，从before属性中获取配置信息
                            tableProcessDim = jsonObj.getObject("before", TableProcessDim.class);
                        } else {
                            // 说明从配置表中进行了读取、新增、修改操作，从after属性中获取配置信息
                            tableProcessDim = jsonObj.getObject("after", TableProcessDim.class);
                        }
                        tableProcessDim.setOp(op);
                        return tableProcessDim;
                    }
                }
        ).setParallelism(1);
        return tpDS;
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaStrDS) {
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        //将jsonStr转换为jsonObj
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String db = jsonObj.getString("database");
                        String type = jsonObj.getString("type");
                        String data = jsonObj.getString("data");
                        if ("edu0318".equals(db)
                                && ("insert".equals(type)
                                || "update".equals(type)
                                || "delete".equals(type)
                                || "bootstrap-insert".equals(type))
                                && data != null
                                && data.length() > 2
                        ) {
                            out.collect(jsonObj);
                        }
                    }
                }
        );

        return jsonObjDS;
    }
}
