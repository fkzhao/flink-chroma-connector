package org.apache.flink.chroma.sink.writer;

import java.util.Objects;

public class ChromaWriterState {

    String labelPrefix;
    String database;
    String collection;
    int subtaskId;

    public ChromaWriterState(String labelPrefix) {
        this.labelPrefix = labelPrefix;
    }

    public ChromaWriterState(String labelPrefix, String database, String table, int subtaskId) {
        this.labelPrefix = labelPrefix;
        this.database = database;
        this.collection = table;
        this.subtaskId = subtaskId;
    }

    public String getLabelPrefix() {
        return labelPrefix;
    }

    public String getDatabase() {
        return database;
    }

    public String getCollection() {
        return collection;
    }

    public int getSubtaskId() {
        return subtaskId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ChromaWriterState that = (ChromaWriterState) o;
        return Objects.equals(labelPrefix, that.labelPrefix)
                && Objects.equals(database, that.database)
                && Objects.equals(collection, that.collection)
                && Objects.equals(subtaskId, that.subtaskId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(labelPrefix, database, collection, subtaskId);
    }

    @Override
    public String toString() {
        return "ChromaWriterState{"
                + "labelPrefix='"
                + labelPrefix
                + '\''
                + ", database='"
                + database
                + '\''
                + ", collection='"
                + collection
                + '\''
                + ", subtaskId="
                + subtaskId
                + '}';
    }

}
