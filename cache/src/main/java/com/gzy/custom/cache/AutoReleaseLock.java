package com.gzy.custom.cache;

public interface AutoReleaseLock extends AutoCloseable {

    @Override
    void close();
}
