package com.gzy.custom.cache.localcache;

import com.gzy.custom.cache.AbstractCacheBuilder;
import com.gzy.custom.cache.config.EmbeddedCacheConfig;

public class EmbeddedCacheBuilder<T extends EmbeddedCacheBuilder<T>> extends AbstractCacheBuilder<T> {

    public EmbeddedCacheBuilder() {}

    public static class EmbeddedCacheBuilderImpl extends EmbeddedCacheBuilder<EmbeddedCacheBuilderImpl> {}

    public static EmbeddedCacheBuilderImpl createEmbeddedCacheBuilder() {
        return new EmbeddedCacheBuilderImpl();
    }

    @Override
    public EmbeddedCacheConfig getConfig() {
        if (config == null) {
            config = new EmbeddedCacheConfig();
        }
        return (EmbeddedCacheConfig)config;
    }

    public T limit(int limit) {
        getConfig().setLimit(limit);
        return self();
    }

    public void setLimit(int limit) {
        getConfig().setLimit(limit);
    }

}
