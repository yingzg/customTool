package com.gzy.custom.cache;

import java.io.Closeable;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.gzy.custom.cache.config.CacheConfig;
import com.gzy.custom.cache.exception.CacheInvokeException;
import com.gzy.custom.cache.result.CacheGetResult;
import com.gzy.custom.cache.result.CacheResult;
import com.gzy.custom.cache.result.CacheResultCode;
import com.gzy.custom.cache.result.MultiGetResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface Cache<K, V> extends Closeable {

    Logger logger = LoggerFactory.getLogger(Cache.class);

    default V get(K key) throws CacheInvokeException {
        CacheGetResult<V> result = GET(key);
        if (result.isSuccess()) {
            return result.getValue();
        } else {
            return null;
        }
    }

    default Map<K, V> getAll(Set<? extends K> keys) throws CacheInvokeException {
        MultiGetResult<K, V> cacheGetResults = GET_ALL(keys);
        return cacheGetResults.unwrapValues();
    }

    default void put(K key, V value) {
        PUT(key, value);
    }

    default void put(K key, V value, long expireAfterWrite, TimeUnit timeUnit) {
        PUT(key, value, expireAfterWrite, timeUnit);
    }

    default CacheResult PUT(K key, V value) {
        if (key == null) {
            return CacheResult.FAIL_ILLEGAL_ARGUMENT;
        }
        return PUT(key, value, config().getExpireAfterWriteInMillis(), TimeUnit.MILLISECONDS);
    }

    default void putAll(Map<? extends K, ? extends V> map) {
        PUT_ALL(map);
    }

    default void putAll(Map<? extends K, ? extends V> map, long expireAfterWrite, TimeUnit timeUnit) {
        PUT_ALL(map, expireAfterWrite, timeUnit);
    }

    default CacheResult PUT_ALL(Map<? extends K, ? extends V> map) {
        if (map == null) {
            return CacheResult.FAIL_ILLEGAL_ARGUMENT;
        }
        return PUT_ALL(map, config().getExpireAfterWriteInMillis(), TimeUnit.MILLISECONDS);
    }

    default boolean remove(K key) {
        return REMOVE(key).isSuccess();
    }

    default void removeAll(Set<? extends K> keys) {
        REMOVE_ALL(keys);
    }

    default boolean putIfAbsent(K key, V value) { // 多级缓存MultiLevelCache不支持此方法
        CacheResult result = PUT_IF_ABSENT(key, value, config().getExpireAfterWriteInMillis(), TimeUnit.MILLISECONDS);
        return result.getResultCode() == CacheResultCode.SUCCESS;
    }

    default V computeIfAbsent(K key, Function<K, V> loader) {
        return computeIfAbsent(key, loader, config().isCacheNullValue());
    }

    default AutoReleaseLock tryLock(K key, long expire, TimeUnit timeUnit) {
        if (key == null) {
            return null;
        }
        // 随机生成一个值
        final String uuid = UUID.randomUUID().toString();
        // 过期时间
        final long expireTimestamp = System.currentTimeMillis() + timeUnit.toMillis(expire);
        final CacheConfig config = config();

        AutoReleaseLock lock = () -> { // 创建一把会自动释放资源的锁，实现其 close() 方法
            int unlockCount = 0;
            while (unlockCount++ < config.getTryLockUnlockCount()) {
                if (System.currentTimeMillis() < expireTimestamp) { // 这把锁还没有过期，则删除
                    // 删除对应的 Key 值
                    // 出现的结果：成功，失败，Key 不存在
                    CacheResult unlockResult = REMOVE(key);
                    if (unlockResult.getResultCode() == CacheResultCode.FAIL
                            || unlockResult.getResultCode() == CacheResultCode.PART_SUCCESS) {
                        // 删除对应的 Key 值过程中出现了异常，则重试
                        logger.info("[tryLock] [{} of {}] [{}] unlock failed. Key={}, msg = {}",
                                unlockCount, config.getTryLockUnlockCount(), uuid, key, unlockResult.getMessage());
                        // retry
                    } else if (unlockResult.isSuccess()) { // 释放成功
                        logger.debug("[tryLock] [{} of {}] [{}] successfully release the lock. Key={}",
                                unlockCount, config.getTryLockUnlockCount(), uuid, key);
                        return;
                    } else { // 锁已经被释放了
                        logger.warn("[tryLock] [{} of {}] [{}] unexpected unlock result: Key={}, result={}",
                                unlockCount, config.getTryLockUnlockCount(), uuid, key, unlockResult.getResultCode());
                        return;
                    }
                } else { // 该锁已失效
                    logger.info("[tryLock] [{} of {}] [{}] lock already expired: Key={}",
                            unlockCount, config.getTryLockUnlockCount(), uuid, key);
                    return;
                }
            }
        };

        int lockCount = 0;
        Cache cache = this;
        while (lockCount++ < config.getTryLockLockCount()) {
            // 往 Redis（或者本地） 中存放 Key 值（_#RL#结尾的Key）
            // 返回的结果：成功、已存在、失败
            CacheResult lockResult = cache.PUT_IF_ABSENT(key, uuid, expire, timeUnit);
            if (lockResult.isSuccess()) { // 成功获取到锁
                logger.debug("[tryLock] [{} of {}] [{}] successfully get a lock. Key={}",
                        lockCount, config.getTryLockLockCount(), uuid, key);
                return lock;
            } else if (lockResult.getResultCode() == CacheResultCode.FAIL || lockResult.getResultCode() == CacheResultCode.PART_SUCCESS) {
                logger.info("[tryLock] [{} of {}] [{}] cache access failed during get lock, will inquiry {} times. Key={}, msg={}",
                        lockCount, config.getTryLockLockCount(), uuid,
                        config.getTryLockInquiryCount(), key, lockResult.getMessage());
                // 尝试获取锁的过程中失败了，也就是往 Redis 中存放 Key 值出现异常
                // 这个时候可能 Key 值已经存储了，但是由于其他原因导致返回的结果表示执行失败
                int inquiryCount = 0;
                while (inquiryCount++ < config.getTryLockInquiryCount()) {
                    CacheGetResult inquiryResult = cache.GET(key);
                    if (inquiryResult.isSuccess()) {
                        if (uuid.equals(inquiryResult.getValue())) {
                            logger.debug("[tryLock] [{} of {}] [{}] successfully get a lock after inquiry. Key={}",
                                    inquiryCount, config.getTryLockInquiryCount(), uuid, key);
                            return lock;
                        } else {
                            logger.debug("[tryLock] [{} of {}] [{}] not the owner of the lock, return null. Key={}",
                                    inquiryCount, config.getTryLockInquiryCount(), uuid, key);
                            return null;
                        }
                    } else {
                        logger.info("[tryLock] [{} of {}] [{}] inquiry failed. Key={}, msg={}",
                                inquiryCount, config.getTryLockInquiryCount(), uuid, key, inquiryResult.getMessage());
                        // retry inquiry
                    }
                }
            } else { // 已存在表示该锁被其他人占有
                // others holds the lock
                logger.debug("[tryLock] [{} of {}] [{}] others holds the lock, return null. Key={}",
                        lockCount, config.getTryLockLockCount(), uuid, key);
                return null;
            }
        }

        logger.debug("[tryLock] [{}] return null after {} attempts. Key={}", uuid, config.getTryLockLockCount(), key);
        return null;
    }

    default boolean tryLockAndRun(K key, long expire, TimeUnit timeUnit, Runnable action) {
        // Release the lock use Java 7 try-with-resources.
        try (AutoReleaseLock lock = tryLock(key, expire, timeUnit)) { // 尝试获取锁
            if (lock != null) { // 获取到锁则执行下面的任务
                action.run();
                return true;
            } else {
                return false;
            }
            // 执行完锁的操作后会进行资源释放，调用 AutoCloseable 的 close() 方法
        }
    }

    V computeIfAbsent(K key, Function<K, V> loader, boolean cacheNullWhenLoaderReturnNull);

    V computeIfAbsent(K key, Function<K, V> loader, boolean cacheNullWhenLoaderReturnNull, long expireAfterWrite, TimeUnit timeUnit);


    <T> T unwrap(Class<T> clazz);

    /**
     * Clean resources created by this cache.
     */
    @Override
    default void close() {
    }

    CacheResult REMOVE_ALL(Set<? extends K> keys);

    CacheResult REMOVE(K key);

    CacheResult PUT_IF_ABSENT(K key, V value, long expireAfterWrite, TimeUnit timeUnit);

    CacheResult PUT_ALL(Map<? extends K, ? extends V> map, long expireAfterWrite, TimeUnit timeUnit);

    CacheResult PUT(K key, V value, long expireAfterWrite, TimeUnit timeUnit);

    CacheConfig<K, V> config();

    MultiGetResult<K, V> GET_ALL(Set<? extends K> keys);

    CacheGetResult<V> GET(K key);

}
