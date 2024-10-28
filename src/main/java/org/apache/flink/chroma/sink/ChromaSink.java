package org.apache.flink.chroma.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.chroma.conf.ChromaOptions;
import org.apache.flink.chroma.sink.commiter.ChromaAbstractCommittable;
import org.apache.flink.chroma.sink.commiter.ChromaCommittableSerializer;
import org.apache.flink.chroma.sink.commiter.ChromaCommitter;
import org.apache.flink.chroma.sink.writer.ChromaAbstractWriter;
import org.apache.flink.chroma.sink.writer.ChromaWriter;
import org.apache.flink.chroma.sink.writer.ChromaWriterState;
import org.apache.flink.chroma.sink.writer.ChromaWriterStateSerializer;
import org.apache.flink.chroma.sink.writer.serializer.ChromaRecordSerializer;
import org.apache.flink.chroma.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;


@PublicEvolving
public class ChromaSink<IN>
        implements StatefulSink<IN, ChromaWriterState>,
        TwoPhaseCommittingSink<IN, ChromaAbstractCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(ChromaSink.class);
    private final ChromaOptions chromaOptions;
    private final ChromaRecordSerializer<IN> serializer;
    public ChromaSink(
            ChromaOptions chromaOptions,
            ChromaRecordSerializer<IN> serializer) {
        this.chromaOptions = chromaOptions;
        this.serializer = serializer;
    }


    @Override
    public ChromaAbstractWriter createWriter(InitContext context) throws IOException {
        return new ChromaWriter<>(context, Collections.<ChromaWriterState>emptyList(), new SimpleStringSerializer());
    }

    @Override
    public Committer createCommitter() throws IOException {
        return new ChromaCommitter();
    }

    @Override
    public SimpleVersionedSerializer getCommittableSerializer() {
        return new ChromaCommittableSerializer();
    }

    @Override
    public ChromaAbstractWriter restoreWriter(InitContext context, Collection<ChromaWriterState> recoveredState) throws IOException {
        return new ChromaWriter<>(context, recoveredState, new SimpleStringSerializer());
    }

    @Override
    public SimpleVersionedSerializer<ChromaWriterState> getWriterStateSerializer() {
        return new ChromaWriterStateSerializer();
    }

    public static <IN> Builder<IN> builder() {
        return new Builder<>();
    }


    /**
     * build for DorisSink.
     *
     * @param <IN> record type.
     */
    public static class Builder<IN> {
        private ChromaOptions chromaOptions;
        private ChromaRecordSerializer<IN> serializer;

        public Builder<IN> setChromaOptions(ChromaOptions chromaOptions) {
            this.chromaOptions = chromaOptions;
            return this;
        }

        public Builder<IN> setSerializer(ChromaRecordSerializer<IN> serializer) {
            this.serializer = serializer;
            return this;
        }

        public ChromaSink<IN> build() {
            Preconditions.checkNotNull(chromaOptions);
            return new ChromaSink<>(chromaOptions, serializer);
        }
    }

}
