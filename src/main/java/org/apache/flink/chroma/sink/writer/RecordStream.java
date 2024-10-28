package org.apache.flink.chroma.sink.writer;

import java.io.IOException;
import java.io.InputStream;

public class RecordStream extends InputStream {
    private final RecordBuffer recordBuffer;

    @Override
    public int read() throws IOException {
        return 0;
    }

    public RecordStream(int bufferSize, int bufferCount, boolean useCache) {
        if (useCache) {
            this.recordBuffer = new CacheRecordBuffer(bufferSize, bufferCount);
        } else {
            this.recordBuffer = new RecordBuffer(bufferSize, bufferCount);
        }
    }

    public void startInput(boolean isResume) throws IOException {
        // if resume from breakpoint, do not recycle cache buffer
        if (!isResume && recordBuffer instanceof CacheRecordBuffer) {
            ((CacheRecordBuffer) recordBuffer).recycleCache();
        }
        recordBuffer.startBufferData();
    }

    public void endInput() throws IOException {
        recordBuffer.stopBufferData();
    }

    @Override
    public int read(byte[] buff) throws IOException {
        try {
            return recordBuffer.read(buff);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void write(byte[] buff) throws IOException {
        try {
            recordBuffer.write(buff);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
