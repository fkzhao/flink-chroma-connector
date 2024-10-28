package org.apache.flink.chroma.sink.writer.serializer;

import java.io.IOException;
import java.io.Serializable;

public interface ChromaRecordSerializer<T> extends Serializable {
    ChromaRecord serialize(T record) throws IOException;

    default void initial() {}

    default ChromaRecord flush() {
        return ChromaRecord.empty;
    }

    default void close() throws Exception {}
}
