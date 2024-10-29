package org.apache.flink.chroma.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.chroma.ChromaClient;
import org.apache.flink.chroma.conf.ChromaOptions;
import org.apache.flink.chroma.conf.LimiterOptions;
import org.apache.flink.chroma.sink.commiter.ChromaAbstractCommittable;
import org.apache.flink.chroma.sink.commiter.ChromaCommittableSerializer;
import org.apache.flink.chroma.sink.commiter.ChromaCommitter;
import org.apache.flink.chroma.sink.writer.ChromaAbstractWriter;
import org.apache.flink.chroma.sink.writer.ChromaWriter;
import org.apache.flink.chroma.sink.writer.ChromaWriterState;
import org.apache.flink.chroma.sink.writer.ChromaWriterStateSerializer;
import org.apache.flink.chroma.sink.writer.serializer.ChromaRecordSerializer;
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

    private static final Logger logger = LoggerFactory.getLogger(ChromaSink.class);
    private final transient ChromaClient chromaClient;
    private final ChromaOptions chromaOptions;
    private final LimiterOptions limiterOptions;
    private final ChromaRecordSerializer<IN> serializer;
    public ChromaSink(
            ChromaOptions chromaOptions,
            LimiterOptions limiterOptions,
            ChromaRecordSerializer<IN> serializer) {
        Preconditions.checkNotNull(chromaOptions);
        Preconditions.checkNotNull(serializer);
        Preconditions.checkNotNull(chromaOptions.getConnectionUrl());
        Preconditions.checkNotNull(chromaOptions.getCollection());
        this.chromaOptions = chromaOptions;
        this.limiterOptions = limiterOptions;
        this.serializer = serializer;
        chromaClient = ChromaClient.builder()
                .url(this.chromaOptions.getConnectionUrl())
                .authType(this.chromaOptions.getAuthType())
                .authIdentify(this.chromaOptions.getAuthIdentity())
                .build();
        checkChromaState();
        initialize();
    }

    private void checkChromaState() {
        //
        if (!chromaClient.checkTenantAndDatabase(this.chromaOptions.getTenant(), this.chromaOptions.getDatabase())) {
            throw new RuntimeException("Chroma tenant: " + this.chromaOptions.getTenant() + " database: " + this.chromaOptions.getDatabase() + " is not exist.");
        }

        //
        if (!chromaClient.checkCollection(this.chromaOptions.getTenant(), this.chromaOptions.getDatabase(), this.chromaOptions.getCollection())) {
            if (this.chromaOptions.isAutoCreateCollection()) {
                if (!chromaClient.createCollection(this.chromaOptions.getTenant(), this.chromaOptions.getDatabase(), this.chromaOptions.getCollection())) {
                    throw new RuntimeException("Failed to create collection: " + this.chromaOptions.getCollection());
                }
            } else {
                throw new RuntimeException("Chroma collection: " + this.chromaOptions.getCollection() + " is not exist.");
            }
        }
    }

    private void initialize() {

    }


    @Override
    public ChromaAbstractWriter createWriter(InitContext context) throws IOException {
        return new ChromaWriter<>(context, Collections.<ChromaWriterState>emptyList(), serializer, chromaClient, limiterOptions);
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
        return new ChromaWriter<>(context, recoveredState, serializer, chromaClient, limiterOptions);
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
        private LimiterOptions limiterOptions;
        private ChromaRecordSerializer<IN> serializer;

        public Builder<IN> setChromaOptions(ChromaOptions chromaOptions) {
            this.chromaOptions = chromaOptions;
            return this;
        }

        public Builder<IN> setSerializer(ChromaRecordSerializer<IN> serializer) {
            this.serializer = serializer;
            return this;
        }

        public Builder<IN> setLimiterOptions(LimiterOptions limiterOptions) {
            this.limiterOptions = limiterOptions;
            return this;
        }


        public ChromaSink<IN> build() {
            Preconditions.checkNotNull(chromaOptions);
            return new ChromaSink<>(chromaOptions, limiterOptions, serializer);
        }
    }

}
