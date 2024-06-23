package com.gzy.custom.cache;

public interface CacheBuilder {
    /**
     * 构建一个缓存实例对象
     * 
     * @param <K>
     * @param <V>
     * @return 缓存实例对象
     */
    <K, V> Cache<K, V> buildCache();
}
