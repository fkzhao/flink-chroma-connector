package org.apache.flink.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.chroma.conf.ChromaOptions;
import org.apache.flink.chroma.sink.ChromaSink;
import org.apache.flink.chroma.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkApp {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            conf.setString(RestOptions.BIND_PORT, "8081");
            StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

            env.enableCheckpointing(30 * 1000);
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
            env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
            env.getCheckpointConfig().enableUnalignedCheckpoints();

            env.setStateBackend(new EmbeddedRocksDBStateBackend());
            env.getConfig().setUseSnapshotCompression(true);
            DataStream<String> sourceDataStream = env.socketTextStream("localhost", 8888);
            ChromaSink<String> chromaSink = ChromaSink.<String>builder()
                    .setChromaOptions(new ChromaOptions())
                    .setSerializer(new SimpleStringSerializer())
                    .build();
            sourceDataStream.print();
            sourceDataStream.sinkTo(chromaSink);
            env.execute("Flink Chroma Connector");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
