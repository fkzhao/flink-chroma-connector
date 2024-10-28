package org.apache.flink.chroma.sink.writer.serializer;

import java.io.Serializable;

public class ChromaRecord implements Serializable {
    public static ChromaRecord empty = new ChromaRecord();

    private String database;
    private String collection;
    private byte[] row;

    public ChromaRecord() {}

    public ChromaRecord(String database, String table, byte[] row) {
        this.database = database;
        this.collection = table;
        this.row = row;
    }

    public String getTableIdentifier() {
        if (database == null || collection == null) {
            return null;
        }
        return database + "." + collection;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public byte[] getRow() {
        return row;
    }

    public void setRow(byte[] row) {
        this.row = row;
    }

    public static ChromaRecord of(String database, String collection, byte[] row) {
        return new ChromaRecord(database, collection, row);
    }

    public static ChromaRecord of(String tableIdentifier, byte[] row) {
        if (tableIdentifier != null) {
            String[] dbTbl = tableIdentifier.split("\\.");
            if (dbTbl.length == 2) {
                String database = dbTbl[0];
                String collection = dbTbl[1];
                return new ChromaRecord(database, collection, row);
            }
        }
        return null;
    }

    public static ChromaRecord of(byte[] row) {
        return new ChromaRecord(null, null, row);
    }
}
