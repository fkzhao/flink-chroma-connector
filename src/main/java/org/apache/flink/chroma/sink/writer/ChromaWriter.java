package org.apache.flink.chroma.sink.writer;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.chroma.sink.commiter.ChromaCommittable;
import org.apache.flink.chroma.sink.writer.serializer.ChromaRecord;
import org.apache.flink.chroma.sink.writer.serializer.ChromaRecordSerializer;
import org.apache.flink.chroma.sink.writer.serializer.SimpleStringSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ChromaWriter <IN>
        implements ChromaAbstractWriter<IN, ChromaWriterState, ChromaCommittable>  {

    private final ChromaRecordSerializer<IN> serializer;
    private final int subtaskId;

    public ChromaWriter(Sink.InitContext initContext,
                        Collection<ChromaWriterState> state,
                        ChromaRecordSerializer<IN> serializer) {
        this.subtaskId = initContext.getSubtaskId();
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
        List<ChromaWriterState> writerStates = new ArrayList<>();
        ChromaWriterState writerState =
                new ChromaWriterState(
                        "snapshot",
                        "snapshot-database",
                        "snapshot-collection",
                        subtaskId);
        writerStates.add(writerState);
        return writerStates;
    }

    @Override
    public Collection<ChromaCommittable> prepareCommit() throws IOException, InterruptedException {
        List<ChromaCommittable> committableList = new ArrayList<>();
        committableList.add(
                new ChromaCommittable(
                        "127.0.0.1:8000", "default", 100000));
        return committableList;
    }

    @Override
    public void write(IN in, Context context) throws IOException, InterruptedException {
        ChromaRecord chromaRecord = serializer.serialize(in);
        System.out.println(chromaRecord);
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {

    }

    @Override
    public void close() throws Exception {

    }
}
