package com.gzy.custom.cache.config;

public class EmbeddedCacheConfig<K, V> extends CacheConfig<K, V> {
    int DEFAULT_LOCAL_LIMIT = 100;
    /**
     * 本地缓存的缓存实例中的缓存数量
     */
    private int limit = DEFAULT_LOCAL_LIMIT;

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

}
