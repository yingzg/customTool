package com.gzy.custom.cache.localcache;

import com.gzy.custom.cache.config.EmbeddedCacheConfig;

public class CaffeineCacheBuilder<T extends EmbeddedCacheBuilder<T>> extends EmbeddedCacheBuilder<T> {
    public static class CaffeineCacheBuilderImpl extends CaffeineCacheBuilder<CaffeineCacheBuilderImpl> {}

    public static CaffeineCacheBuilderImpl createCaffeineCacheBuilder() {
        return new CaffeineCacheBuilderImpl();
    }

    protected CaffeineCacheBuilder() {
        // 设置构建 CaffeineCache 缓存实例的函数
        buildFunc((c) -> new CaffeineCache((EmbeddedCacheConfig)c));
    }
}
