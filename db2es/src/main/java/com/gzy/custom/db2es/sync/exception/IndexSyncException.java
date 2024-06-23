package com.gzy.custom.db2es.sync.exception;


public class IndexSyncException extends RuntimeException {

    public IndexSyncException(String message) {
        super(message);
    }
}