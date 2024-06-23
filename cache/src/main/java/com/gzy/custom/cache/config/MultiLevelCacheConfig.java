package com.gzy.custom.cache.config;

import com.gzy.custom.cache.Cache;

import java.util.ArrayList;
import java.util.List;


public class MultiLevelCacheConfig<K, V> extends CacheConfig<K, V> {
    private List<Cache<K, V>> caches = new ArrayList<>();
    /**
     * 两级缓存时，本地缓存是否单独设置过期时间
     */
    private boolean useExpireOfSubCache;

    @Override
    public MultiLevelCacheConfig clone() {
        MultiLevelCacheConfig copy = (MultiLevelCacheConfig) super.clone();
        if (caches != null) {
            copy.caches = new ArrayList(this.caches);
        }
        return copy;
    }

    public List<Cache<K, V>> getCaches() {
        return caches;
    }

    public void setCaches(List<Cache<K, V>> caches) {
        this.caches = caches;
    }

    public boolean isUseExpireOfSubCache() {
        return useExpireOfSubCache;
    }

    public void setUseExpireOfSubCache(boolean useExpireOfSubCache) {
        this.useExpireOfSubCache = useExpireOfSubCache;
    }
}

