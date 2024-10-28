package org.apache.flink.chroma.sink.commiter;

import org.apache.flink.api.connector.sink2.Committer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;

public class ChromaCommitter implements Committer<ChromaCommittable>, Closeable {
    @Override
    public void commit(Collection<CommitRequest<ChromaCommittable>> committable) throws IOException, InterruptedException {
        System.out.println(" ChromaCommitter commit");
    }

    @Override
    public void close() throws IOException {

    }
}
