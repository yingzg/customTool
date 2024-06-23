package com.gzy.custom.cache.config;

import java.time.Duration;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.api.StatefulConnection;

public class RedisLettuceCacheConfig<K, V> extends ExternalCacheConfig<K, V> {

    Duration ASYNC_RESULT_TIMEOUT = Duration.ofMillis(1000);
    /**
     * Redis客户端
     */
    private AbstractRedisClient redisClient;
    /**
     * 线程安全连接
     */
    private StatefulConnection connection;
    /**
     * 异步超时时间
     */
    private long asyncResultTimeoutInMillis = ASYNC_RESULT_TIMEOUT.toMillis();

    public AbstractRedisClient getRedisClient() {
        return redisClient;
    }

    public void setRedisClient(AbstractRedisClient redisClient) {
        this.redisClient = redisClient;
    }

    public StatefulConnection getConnection() {
        return connection;
    }

    public void setConnection(StatefulConnection connection) {
        this.connection = connection;
    }

    public long getAsyncResultTimeoutInMillis() {
        return asyncResultTimeoutInMillis;
    }

    public void setAsyncResultTimeoutInMillis(long asyncResultTimeoutInMillis) {
        this.asyncResultTimeoutInMillis = asyncResultTimeoutInMillis;
    }
}
