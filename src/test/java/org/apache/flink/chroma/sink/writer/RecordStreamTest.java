package org.apache.flink.chroma.sink.writer;

import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class RecordStreamTest {
    RecordStream recordStream = new RecordStream(10*1024*1024, 100, false);
    @Test
    public void testRecordStream() throws IOException {
        recordStream.startInput(false);
        recordStream.write("Hello World".getBytes(StandardCharsets.UTF_8));
        recordStream.write("!".getBytes(StandardCharsets.UTF_8));
        recordStream.endInput();
        byte[] s = new byte[5];
        int size = recordStream.read(s);
        System.out.println("Read size:["+  size +"] String result: ["+ new String(s, StandardCharsets.UTF_8) + "]");
        s = new byte[5];
        size = recordStream.read(s);
        System.out.println("Read size:["+  size +"] String result: ["+ new String(s, StandardCharsets.UTF_8) + "]");
        s = new byte[5];
        size = recordStream.read(s);
        System.out.println("Read size:["+  size +"] String result: ["+ new String(s, 0, size, StandardCharsets.UTF_8) + "]");
        s = new byte[5];
        size = recordStream.read(s);
        Assertions.assertEquals(size, -1);
        Assertions.assertEquals(recordStream.available(), 0);

    }
}
