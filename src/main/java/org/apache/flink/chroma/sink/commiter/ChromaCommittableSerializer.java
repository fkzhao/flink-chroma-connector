package org.apache.flink.chroma.sink.commiter;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;

public class ChromaCommittableSerializer implements SimpleVersionedSerializer<ChromaCommittable> {
    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(ChromaCommittable chromaCommittable) throws IOException {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final DataOutputStream out = new DataOutputStream(baos)) {
            out.writeUTF(chromaCommittable.getHostPort());
            out.writeUTF(chromaCommittable.getDb());
            out.writeLong(chromaCommittable.getTxnID());

            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public ChromaCommittable deserialize(int version, byte[] serialized) throws IOException {
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             final DataInputStream in = new DataInputStream(bais)) {
            final String hostPort = in.readUTF();
            final String db = in.readUTF();
            final long txnId = in.readLong();
            return new ChromaCommittable(hostPort, db, txnId);
        }
    }
}
