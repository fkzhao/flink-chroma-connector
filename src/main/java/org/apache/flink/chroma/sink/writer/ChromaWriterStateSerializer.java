package org.apache.flink.chroma.sink.writer;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;

public class ChromaWriterStateSerializer implements SimpleVersionedSerializer<ChromaWriterState> {
    @Override
    public int getVersion() {
        return 2;
    }

    @Override
    public byte[] serialize(ChromaWriterState chromaWriterState) throws IOException {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final DataOutputStream out = new DataOutputStream(baos)) {
            out.writeUTF(chromaWriterState.getLabelPrefix());
            out.writeUTF(chromaWriterState.getDatabase());
            out.writeUTF(chromaWriterState.getCollection());
            out.writeInt(chromaWriterState.getSubtaskId());
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public ChromaWriterState deserialize(int version, byte[] serialized) throws IOException {
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             final DataInputStream in = new DataInputStream(bais)) {
            String labelPrefix = in.readUTF();
            if (version == 1) {
                return new ChromaWriterState(labelPrefix);
            } else {
                final String database = in.readUTF();
                final String collection = in.readUTF();
                final int subtaskId = in.readInt();
                return new ChromaWriterState(labelPrefix, database, collection, subtaskId);
            }
        }
    }
}
