package org.apache.flink.chroma.sink.writer.serializer;

import com.alibaba.fastjson.JSON;
import org.apache.flink.chroma.sink.entity.ChromaSinkData;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ChromaSinkDataSerializer implements ChromaRecordSerializer<ChromaSinkData>{
    @Override
    public ChromaRecord serialize(ChromaSinkData record) throws IOException {
        return  ChromaRecord.of(JSON.toJSONString(record).getBytes(StandardCharsets.UTF_8));
    }
}
