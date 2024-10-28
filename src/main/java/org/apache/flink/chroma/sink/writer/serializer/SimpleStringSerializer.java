package org.apache.flink.chroma.sink.writer.serializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class SimpleStringSerializer implements ChromaRecordSerializer<String> {

    @Override
    public ChromaRecord serialize(String record) throws IOException {
        return ChromaRecord.of(record.getBytes(StandardCharsets.UTF_8));
    }
}
