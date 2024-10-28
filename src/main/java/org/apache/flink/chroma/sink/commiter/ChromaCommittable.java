package org.apache.flink.chroma.sink.commiter;

import java.util.Objects;

public class ChromaCommittable implements ChromaAbstractCommittable{

    private final String hostPort;
    private final String db;
    private final long txnID;

    public ChromaCommittable(String hostPort, String db, long txnID) {
        this.hostPort = hostPort;
        this.db = db;
        this.txnID = txnID;
    }

    public String getHostPort() {
        return hostPort;
    }

    public String getDb() {
        return db;
    }

    public long getTxnID() {
        return txnID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ChromaCommittable that = (ChromaCommittable) o;
        return txnID == that.txnID
                && Objects.equals(hostPort, that.hostPort)
                && Objects.equals(db, that.db);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hostPort, db, txnID);
    }

    @Override
    public String toString() {
        return "ChromaCommittable{"
                + "hostPort='"
                + hostPort
                + '\''
                + ", db='"
                + db
                + '\''
                + ", txnID="
                + txnID
                + '}';
    }
}
