package com.gzy.custom.cache;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class CacheUtil {

    private interface ProxyLoader<K, V> extends CacheLoader<K, V> {
    }

    public static <K, V> ProxyLoader<K, V> createProxyLoader(Cache<K, V> cache, CacheLoader<K, V> loader) {
        if (loader instanceof ProxyLoader) {
            return (ProxyLoader<K, V>) loader;
        }
        // 对CacheLoader进行封装
        return new ProxyLoader<K, V>() {
            @Override
            public V load(K key) throws Throwable {
                long t = System.currentTimeMillis();
                V v = null;
                boolean success = false;
                try {
                    // 调用原有方法，获取返回结果
                    v = loader.load(key);
                    // 执行结束
                    success = true;
                } finally {

                }
                return v;
            }

            @Override
            public Map<K, V> loadAll(Set<K> keys) throws Throwable {
                long t = System.currentTimeMillis();
                boolean success = false;
                Map<K, V> kvMap = null;
                try {
                    kvMap = loader.loadAll(keys);
                    success = true;
                } finally {

                }
                return kvMap;
            }

            @Override
            public boolean vetoCacheUpdate() {
                return loader.vetoCacheUpdate();
            }
        };
    }

    public static <K, V> ProxyLoader<K, V> createProxyLoader(Cache<K, V> cache, Function<K, V> loader
                                                            ) {
        if (loader instanceof ProxyLoader) {
            return (ProxyLoader<K, V>) loader;
        }
        if (loader instanceof CacheLoader) { // 生成代理对象，用于执行方法并统计
            return createProxyLoader(cache, (CacheLoader) loader);
        }
        return k -> {
            long t = System.currentTimeMillis();
            V v = null;
            boolean success = false;
            try {
                // 调用原有方法，获取返回结果
                v = loader.apply(k);
                // 执行结束
                success = true;
            } finally {
            }
            return v;
        };
    }

    public static <K, V> AbstractCache<K, V> getAbstractCache(Cache<K, V> c) {
        while (c instanceof ProxyCache) {
            c = ((ProxyCache) c).getTargetCache();
        }
        return (AbstractCache) c;
    }

}
