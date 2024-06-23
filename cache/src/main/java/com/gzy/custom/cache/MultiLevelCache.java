package com.gzy.custom.cache;

import com.gzy.custom.cache.config.CacheConfig;
import com.gzy.custom.cache.config.MultiLevelCacheConfig;
import com.gzy.custom.cache.exception.CacheConfigException;
import com.gzy.custom.cache.result.CacheGetResult;
import com.gzy.custom.cache.result.CacheResult;
import com.gzy.custom.cache.result.CacheResultCode;
import com.gzy.custom.cache.result.MultiGetResult;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;


public class MultiLevelCache<K, V> extends AbstractCache<K, V> {

    private Cache[] caches;

    private MultiLevelCacheConfig<K, V> config;

    @SuppressWarnings("unchecked")
    @Deprecated
    public MultiLevelCache(Cache... caches) throws CacheConfigException {
        this.caches = caches;
        checkCaches();
        CacheConfig lastConfig = caches[caches.length - 1].config();
        config = new MultiLevelCacheConfig<>();
        config.setCaches(Arrays.asList(caches));
        config.setExpireAfterWriteInMillis(lastConfig.getExpireAfterWriteInMillis());
        config.setCacheNullValue(lastConfig.isCacheNullValue());
    }

    @SuppressWarnings("unchecked")
    public MultiLevelCache(MultiLevelCacheConfig<K, V> cacheConfig) throws CacheConfigException {
        this.config = cacheConfig;
        this.caches = cacheConfig.getCaches().toArray(new Cache[] {});
        checkCaches();
    }

    private void checkCaches() {
        if (caches == null || caches.length == 0) {
            throw new IllegalArgumentException();
        }
        for (Cache c : caches) {
            if (c.config().getLoader() != null) {
                throw new CacheConfigException(
                    "Loader on sub cache is not allowed, set the loader into MultiLevelCache.");
            }
        }
    }

    public Cache[] caches() {
        return caches;
    }

    @Override
    public MultiLevelCacheConfig<K, V> config() {
        return config;
    }

    @Override
    public CacheResult PUT(K key, V value) {
        if (config.isUseExpireOfSubCache()) { // 本地缓存使用自己的失效时间
            // 设置了TimeUnit为null，本地缓存则使用自己的到期时间
            return PUT(key, value, 0, null);
        } else {
            return PUT(key, value, config().getExpireAfterWriteInMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public CacheResult PUT_ALL(Map<? extends K, ? extends V> map) {
        if (config.isUseExpireOfSubCache()) {
            return PUT_ALL(map, 0, null);
        } else {
            return PUT_ALL(map, config().getExpireAfterWriteInMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @Override
    protected CacheGetResult<V> do_GET(K key) {
        // 遍历多级缓存（远程缓存排在后面）
        for (int i = 0; i < caches.length; i++) {
            Cache cache = caches[i];
            CacheGetResult result = cache.GET(key);
            if (result.isSuccess()) {
                CacheValueHolder<V> holder = unwrapHolder(result.getHolder());
                /*
                 * 这个遍历是从低层的缓存开始获取，获取成功则将该值设置到更低层的缓存中
                 * 情景：
                 * 本地没有获取到缓存，远程获取到了缓存，这里会将远程的缓存数据设置到本地中，
                 * 这样下次请求则直接从本次获取，减少了远程获取的时间
                 */
                checkResultAndFillUpperCache(key, i, holder);
                return new CacheGetResult(CacheResultCode.SUCCESS, null, holder);
            }
        }
        return CacheGetResult.NOT_EXISTS_WITHOUT_MSG;
    }

    private CacheValueHolder<V> unwrapHolder(CacheValueHolder<V> h) {
        // if @Cached or @CacheCache change type from REMOTE to BOTH (or from BOTH to REMOTE),
        // during the dev/publish process, the value type which different application server put into cache server will
        // be different
        // (CacheValueHolder<V> and CacheValueHolder<CacheValueHolder<V>>, respectively).
        // So we need correct the problem at here and in CacheGetResult.
        Objects.requireNonNull(h);
        if (h.getValue() instanceof CacheValueHolder) {
            return (CacheValueHolder<V>)h.getValue();
        } else {
            return h;
        }
    }

    private void checkResultAndFillUpperCache(K key, int i, CacheValueHolder<V> h) {
        Objects.requireNonNull(h);
        long currentExpire = h.getExpireTime();
        long now = System.currentTimeMillis();
        if (now <= currentExpire) {
            if (config.isUseExpireOfSubCache()) { // 如果使用本地自己的缓存过期时间
                // 使用本地缓存自己的过期时间
                PUT_caches(i, key, h.getValue(), 0, null);
            } else { // 使用远程缓存的过期时间
                long restTtl = currentExpire - now;
                if (restTtl > 0) { // 远程缓存数据还未失效，则重新设置本地的缓存
                    PUT_caches(i, key, h.getValue(), restTtl, TimeUnit.MILLISECONDS);
                }
            }
        }
    }

    @Override
    protected MultiGetResult<K, V> do_GET_ALL(Set<? extends K> keys) {
        HashMap<K, CacheGetResult<V>> resultMap = new HashMap<>();
        Set<K> restKeys = new HashSet<>(keys);
        for (int i = 0; i < caches.length; i++) {
            if (restKeys.size() == 0) {
                break;
            }
            Cache<K, CacheValueHolder<V>> c = caches[i];
            MultiGetResult<K, CacheValueHolder<V>> allResult = c.GET_ALL(restKeys);
            if (allResult.isSuccess() && allResult.getValues() != null) {
                for (Map.Entry<K, CacheGetResult<CacheValueHolder<V>>> en : allResult.getValues().entrySet()) {
                    K key = en.getKey();
                    CacheGetResult result = en.getValue();
                    if (result.isSuccess()) {
                        CacheValueHolder<V> holder = unwrapHolder(result.getHolder());
                        checkResultAndFillUpperCache(key, i, holder);
                        resultMap.put(key, new CacheGetResult(CacheResultCode.SUCCESS, null, holder));
                        restKeys.remove(key);
                    }
                }
            }
        }
        for (K k : restKeys) {
            resultMap.put(k, CacheGetResult.NOT_EXISTS_WITHOUT_MSG);
        }
        return new MultiGetResult<>(CacheResultCode.SUCCESS, null, resultMap);
    }

    @Override
    protected CacheResult do_PUT(K key, V value, long expireAfterWrite, TimeUnit timeUnit) {
        return PUT_caches(caches.length, key, value, expireAfterWrite, timeUnit);
    }

    @Override
    protected CacheResult do_PUT_ALL(Map<? extends K, ? extends V> map, long expireAfterWrite, TimeUnit timeUnit) {
        CompletableFuture<ResultData> future = CompletableFuture.completedFuture(null);
        for (Cache c : caches) {
            CacheResult r;
            if (timeUnit == null) {
                r = c.PUT_ALL(map);
            } else {
                r = c.PUT_ALL(map, expireAfterWrite, timeUnit);
            }
            future = combine(future, r);
        }
        return new CacheResult(future);
    }

    private CacheResult PUT_caches(int lastIndex, K key, V value, long expire, TimeUnit timeUnit) {
        CompletableFuture<ResultData> future = CompletableFuture.completedFuture(null);
        for (int i = 0; i < lastIndex; i++) {
            Cache cache = caches[i];
            CacheResult r;
            if (timeUnit == null) { // 表示本地缓存使用自己过期时间
                r = cache.PUT(key, value);
            } else {
                r = cache.PUT(key, value, expire, timeUnit);
            }
            // 将多个 PUT 操作放在一条链上
            future = combine(future, r);
        }
        return new CacheResult(future);
    }

    private CompletableFuture<ResultData> combine(CompletableFuture<ResultData> future, CacheResult result) {
        return future.thenCombine(result.future(), (d1, d2) -> {
            if (d1 == null) {
                return d2;
            }
            if (d1.getResultCode() != d2.getResultCode()) {
                return new ResultData(CacheResultCode.PART_SUCCESS, null, null);
            }
            return d1;
        });
    }

    @Override
    protected CacheResult do_REMOVE(K key) {
        CompletableFuture<ResultData> future = CompletableFuture.completedFuture(null);
        for (Cache cache : caches) {
            CacheResult r = cache.REMOVE(key);
            future = combine(future, r);
        }
        return new CacheResult(future);
    }

    @Override
    protected CacheResult do_REMOVE_ALL(Set<? extends K> keys) {
        CompletableFuture<ResultData> future = CompletableFuture.completedFuture(null);
        for (Cache cache : caches) {
            CacheResult r = cache.REMOVE_ALL(keys);
            future = combine(future, r);
        }
        return new CacheResult(future);
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        Objects.requireNonNull(clazz);
        for (Cache cache : caches) {
            try {
                T obj = (T)cache.unwrap(clazz);
                if (obj != null) {
                    return obj;
                }
            } catch (IllegalArgumentException e) {
                // ignore
            }
        }
        throw new IllegalArgumentException(clazz.getName());
    }

    @Override
    public AutoReleaseLock tryLock(K key, long expire, TimeUnit timeUnit) {
        if (key == null) {
            return null;
        }
        return caches[caches.length - 1].tryLock(key, expire, timeUnit);
    }

    @Override
    public boolean putIfAbsent(K key, V value) {
        throw new UnsupportedOperationException("putIfAbsent is not supported by MultiLevelCache");
    }

    @Override
    protected CacheResult do_PUT_IF_ABSENT(K key, V value, long expireAfterWrite, TimeUnit timeUnit) {
        throw new UnsupportedOperationException("PUT_IF_ABSENT is not supported by MultiLevelCache");
    }

    @Override
    public void close() {
        for (Cache c : caches) {
            c.close();
        }
    }
}
