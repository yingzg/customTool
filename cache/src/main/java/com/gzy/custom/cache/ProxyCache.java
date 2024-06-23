package com.gzy.custom.cache;

public interface ProxyCache<K, V> extends Cache<K, V> {
    Cache<K, V> getTargetCache();

    @Override
    default <T> T unwrap(Class<T> clazz) {
        return getTargetCache().unwrap(clazz);
    }

}
