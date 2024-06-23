package com.gzy.custom.cache.localcache;

import com.gzy.custom.cache.config.EmbeddedCacheConfig;

public class LinkedHashMapCacheBuilder<T extends EmbeddedCacheBuilder<T>> extends EmbeddedCacheBuilder<T> {
    public static class LinkedHashMapCacheBuilderImpl
        extends LinkedHashMapCacheBuilder<LinkedHashMapCacheBuilderImpl> {}

    public static LinkedHashMapCacheBuilderImpl createLinkedHashMapCacheBuilder() {
        return new LinkedHashMapCacheBuilderImpl();
    }

    protected LinkedHashMapCacheBuilder() {
        buildFunc((c) -> new LinkedHashMapCache((EmbeddedCacheConfig)c));
    }
}
