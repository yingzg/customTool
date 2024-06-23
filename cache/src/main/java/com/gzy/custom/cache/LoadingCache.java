package com.gzy.custom.cache;

import com.gzy.custom.cache.config.CacheConfig;
import com.gzy.custom.cache.exception.CacheInvokeException;
import com.gzy.custom.cache.result.CacheResultCode;
import com.gzy.custom.cache.result.MultiGetResult;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class LoadingCache<K, V> extends SimpleProxyCache<K, V> {


    protected CacheConfig<K, V> config;

    public LoadingCache(Cache<K, V> cache) {
        super(cache);
        this.config = config();
    }

    @Override
    public V get(K key) throws CacheInvokeException {
        CacheLoader<K, V> loader = config.getLoader();
        if (loader != null) {
            return AbstractCache.computeIfAbsentImpl(key, loader,
                    config.isCacheNullValue() ,0, null, this);
        } else {
            return cache.get(key);
        }
    }

    protected boolean needUpdate(V loadedValue, CacheLoader<K, V> loader) {
        if (loadedValue == null && !config.isCacheNullValue()) {
            return false;
        }
        if (loader.vetoCacheUpdate()) {
            return false;
        }
        return true;
    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys) throws CacheInvokeException {
        CacheLoader<K, V> loader = config.getLoader();
        if (loader != null) {
            MultiGetResult<K, V> r = GET_ALL(keys);
            Map<K, V> kvMap;
            if (r.isSuccess() || r.getResultCode() == CacheResultCode.PART_SUCCESS) {
                kvMap = r.unwrapValues();
            } else {
                kvMap = new HashMap<>();
            }
            Set<K> keysNeedLoad = new HashSet<>();
            keys.forEach((k) -> {
                if (!kvMap.containsKey(k)) {
                    keysNeedLoad.add(k);
                }
            });
            if (!config.isCachePenetrationProtect()) {
                loader = CacheUtil.createProxyLoader(cache, loader);
                Map<K, V> loadResult;
                try {
                    loadResult = loader.loadAll(keysNeedLoad);

                    CacheLoader<K, V> theLoader = loader;
                    Map<K, V> updateValues = loadResult.entrySet().stream()
                            .filter(kvEntry -> needUpdate(kvEntry.getValue(), theLoader))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                    // batch put
                    if (!updateValues.isEmpty()) {
                        PUT_ALL(updateValues);
                    }
                } catch (Throwable e) {
                    throw new CacheInvokeException(e);
                }
                kvMap.putAll(loadResult);
            } else {
                AbstractCache<K, V> abstractCache = CacheUtil.getAbstractCache(cache);
                loader = CacheUtil.createProxyLoader(cache, loader);
                for(K key : keysNeedLoad) {
                    Consumer<V> cacheUpdater = (v) -> {
                        if(needUpdate(v, config.getLoader())) {
                            PUT(key, v);
                        }
                    };
                    V v = AbstractCache.synchronizedLoad(config, abstractCache, key, loader, cacheUpdater);
                    kvMap.put(key, v);
                }
            }
            return kvMap;
        } else {
            return cache.getAll(keys);
        }

    }
}

