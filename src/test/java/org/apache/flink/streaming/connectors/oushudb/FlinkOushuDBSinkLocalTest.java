package org.apache.flink.streaming.connectors.oushudb;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.oushudb.internals.OushuDBTableInfo;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.VarCharType;

import java.util.Properties;

public class FlinkOushuDBSinkLocalTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        CheckpointConfig ckpConfig = env.getCheckpointConfig();
        ckpConfig.setCheckpointInterval(10_000);
        ckpConfig.enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        ckpConfig.setCheckpointStorage("hdfs://aiserver-1:9000/flink/checkpoint");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");

        DataStream<String> stream =
                env.addSource(
                        new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties));

        DataStream<RowData> out =
                stream.map(
                        (MapFunction<String, RowData>)
                                a -> {
                                    String[] s = a.split(",");
                                    return GenericRowData.of(
                                            StringData.fromString(s[0]), Integer.valueOf(s[1]));
                                });
        OushuDBTableInfo.Field[] fields =
                new OushuDBTableInfo.Field[] {
                    new OushuDBTableInfo.Field("a", new VarCharType()),
                    new OushuDBTableInfo.Field("b", new IntType())
                };
        OushuDBTableInfo tableInfo = new OushuDBTableInfo("public", "test", fields);
        Properties config = new Properties();
        config.setProperty(FlinkOushuDBSink.OUSHUDB_HOSTS, "aiserver-1");
        config.setProperty(FlinkOushuDBSink.OUSHUDB_PORT, "5432");
        config.setProperty(FlinkOushuDBSink.OUSHUDB_DATABASE, "flink");
        config.setProperty(FlinkOushuDBSink.OUSHUDB_USER, "oushu");
        config.setProperty(FlinkOushuDBSink.OUSHUDB_PASSWORD, "0ushuCloud");
        config.setProperty(
                FlinkOushuDBSink.OUSHUDB_TEMP_PATH, "hdfs://hdfs@aiserver-1:9000/flink/data");

        FlinkOushuDBSink sink = new FlinkOushuDBSink(tableInfo, config);
        out.addSink(sink).name("testOushuDBSink");
        env.execute();
    }
}
