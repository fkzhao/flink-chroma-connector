package org.apache.flink.chroma.sink.writer.serializer;

import com.alibaba.fastjson.JSONObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class JSONObjectSerializer implements ChromaRecordSerializer<JSONObject>{
    @Override
    public ChromaRecord serialize(JSONObject record) throws IOException {
        return  ChromaRecord.of(record.toJSONString().getBytes(StandardCharsets.UTF_8));
    }
}
