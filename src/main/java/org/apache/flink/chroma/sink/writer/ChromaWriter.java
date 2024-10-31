package org.apache.flink.chroma.sink.writer;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.chroma.ChromaClient;
import org.apache.flink.chroma.ChromaCollection;
import org.apache.flink.chroma.conf.LimiterOptions;
import org.apache.flink.chroma.sink.commiter.ChromaCommittable;
import org.apache.flink.chroma.sink.writer.serializer.ChromaRecord;
import org.apache.flink.chroma.sink.writer.serializer.ChromaRecordSerializer;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class ChromaWriter<IN>
        implements ChromaAbstractWriter<IN, ChromaWriterState, ChromaCommittable> {
    private static final Logger logger = LoggerFactory.getLogger(ChromaWriter.class);
    private final transient ScheduledExecutorService scheduledExecutorService;
    private final ChromaRecordSerializer<IN> serializer;
    private final int subtaskId;
    private final ChromaCollection chromaCollection;
    private final LimiterOptions limiterOptions;
//    private final RecordStream recordStream;
    private boolean loadBatchFirstRecord;
    private final byte[] lineDelimiter;


    public ChromaWriter(Sink.InitContext initContext,
                        Collection<ChromaWriterState> state,
                        ChromaRecordSerializer<IN> serializer,
                        ChromaCollection chromaCollection,
                        LimiterOptions limiterOptions) {
        this.chromaCollection = chromaCollection;
        this.limiterOptions = limiterOptions;
        this.scheduledExecutorService =
                new ScheduledThreadPoolExecutor(1, new ExecutorThreadFactory("stream-load-check"));
        this.subtaskId = initContext.getSubtaskId();
//        this.recordStream =
//                new RecordStream(
//                        10 * 1024 * 1024,
//                       100,
//                        true);
        lineDelimiter = "\n".getBytes();
        loadBatchFirstRecord = true;
        initializeLoad(state);
        this.serializer = serializer;
        serializer.initial();
    }

    public void initializeLoad(Collection<ChromaWriterState> recoveredStates) {
        for (ChromaWriterState state : recoveredStates) {
            System.out.println(state);
        }
    }

    @Override
    public List<ChromaWriterState> snapshotState(long checkpointId) throws IOException {
        return Collections.<ChromaWriterState>emptyList();
    }

    @Override
    public Collection<ChromaCommittable> prepareCommit() throws IOException, InterruptedException {
        return  Collections.<ChromaCommittable>emptyList();
    }

    @Override
    public void write(IN in, Context context) throws IOException, InterruptedException {
        writeOneChromaRecord(serializer.serialize(in));
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        writeOneChromaRecord(serializer.flush());
    }

    @Override
    public void close() throws Exception {
        logger.info("Close ChromaWriter.");
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
        }
        serializer.close();
    }

    public void writeOneChromaRecord(ChromaRecord record) throws IOException {
        if (record == null || record.getRow() == null) {
            return;
        }
        String document = "{\"embeddings\":[[]],\"metadatas\":[{\"name\":\"demo\"}],\"documents\":[\"this is test documents\"],\"uris\":[\"string\"],\"ids\":[\"id1\"]}";
        chromaCollection.upsertDocument(document);
    }


    public void writeOneChromaRecordWithCache(ChromaRecord record) throws IOException, InterruptedException {
//        if (record == null || record.getRow() == null) {
//            return;
//        }
//        if (loadBatchFirstRecord) {
//            loadBatchFirstRecord = false;
//        } else if (lineDelimiter != null) {
//            recordStream.write(lineDelimiter);
//        }
//        recordStream.write(record.getRow());
    }
}
