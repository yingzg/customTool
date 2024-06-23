package com.gzy.custom.cache.config;

import com.gzy.custom.cache.CacheLoader;
import com.gzy.custom.cache.RefreshPolicy;
import com.gzy.custom.cache.exception.CacheException;

import java.time.Duration;
import java.util.function.Function;


public class CacheConfig<K, V> implements Cloneable {

    int DEFAULT_EXPIRE = Integer.MAX_VALUE;
    /**
     * 缓存有效时间
     */
    private long expireAfterWriteInMillis = DEFAULT_EXPIRE * 1000L;
    /**
     * 缓存在访问后多久失效
     */
    private long expireAfterAccessInMillis = 0;
    /**
     * Key 转换函数
     */
    private Function<K, Object> keyConvertor;

    private CacheLoader<K, V> loader;

    /**
     * 是否缓存 null 值
     */
    private boolean cacheNullValue = false;

    /**
     * 刷新策略
     */
    private RefreshPolicy refreshPolicy;

    /**
     * 尝试释放分布式锁的次数
     */
    private int tryLockUnlockCount = 2;
    /**
     * 尝试获取分布式锁出现异常允许访问的次数
     */
    private int tryLockInquiryCount = 1;
    /**
     * 尝试获取分布式锁的次数
     */
    private int tryLockLockCount = 2;

    private boolean cachePenetrationProtect = false;
    private Duration penetrationProtectTimeout = null;

    @Override
    public CacheConfig clone() {
        try {
            CacheConfig copy = (CacheConfig)super.clone();
            if (refreshPolicy != null) {
                copy.refreshPolicy = this.refreshPolicy.clone();
            }
            return copy;
        } catch (CloneNotSupportedException e) {
            throw new CacheException(e);
        }
    }

    public Function<K, Object> getKeyConvertor() {
        return keyConvertor;
    }

    public void setKeyConvertor(Function<K, Object> keyConvertor) {
        this.keyConvertor = keyConvertor;
    }

    public boolean isExpireAfterAccess() {
        return expireAfterAccessInMillis > 0;
    }

    public boolean isExpireAfterWrite() {
        return expireAfterWriteInMillis > 0;
    }

    @Deprecated
    public long getDefaultExpireInMillis() {
        return expireAfterWriteInMillis;
    }

    @Deprecated
    public void setDefaultExpireInMillis(long defaultExpireInMillis) {
        this.expireAfterWriteInMillis = defaultExpireInMillis;
    }

    public long getExpireAfterWriteInMillis() {
        return expireAfterWriteInMillis;
    }

    public void setExpireAfterWriteInMillis(long expireAfterWriteInMillis) {
        this.expireAfterWriteInMillis = expireAfterWriteInMillis;
    }

    public long getExpireAfterAccessInMillis() {
        return expireAfterAccessInMillis;
    }

    public void setExpireAfterAccessInMillis(long expireAfterAccessInMillis) {
        this.expireAfterAccessInMillis = expireAfterAccessInMillis;
    }

    public CacheLoader<K, V> getLoader() {
        return loader;
    }

    public void setLoader(CacheLoader<K, V> loader) {
        this.loader = loader;
    }

    public boolean isCacheNullValue() {
        return cacheNullValue;
    }

    public void setCacheNullValue(boolean cacheNullValue) {
        this.cacheNullValue = cacheNullValue;
    }

    public RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    public void setRefreshPolicy(RefreshPolicy refreshPolicy) {
        this.refreshPolicy = refreshPolicy;
    }

    public int getTryLockUnlockCount() {
        return tryLockUnlockCount;
    }

    public void setTryLockUnlockCount(int tryLockUnlockCount) {
        this.tryLockUnlockCount = tryLockUnlockCount;
    }

    public int getTryLockInquiryCount() {
        return tryLockInquiryCount;
    }

    public void setTryLockInquiryCount(int tryLockInquiryCount) {
        this.tryLockInquiryCount = tryLockInquiryCount;
    }

    public int getTryLockLockCount() {
        return tryLockLockCount;
    }

    public void setTryLockLockCount(int tryLockLockCount) {
        this.tryLockLockCount = tryLockLockCount;
    }

    public boolean isCachePenetrationProtect() {
        return cachePenetrationProtect;
    }

    public void setCachePenetrationProtect(boolean cachePenetrationProtect) {
        this.cachePenetrationProtect = cachePenetrationProtect;
    }

    public Duration getPenetrationProtectTimeout() {
        return penetrationProtectTimeout;
    }

    public void setPenetrationProtectTimeout(Duration penetrationProtectTimeout) {
        this.penetrationProtectTimeout = penetrationProtectTimeout;
    }
}
