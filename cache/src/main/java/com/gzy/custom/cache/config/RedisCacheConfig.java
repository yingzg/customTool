package com.gzy.custom.cache.config;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.util.Pool;

public class RedisCacheConfig<K, V> extends ExternalCacheConfig<K, V> {

    /**
     * Redis 连接池
     */
    private Pool<Jedis> jedisPool;
    /**
     * Redis 从节点的连接池
     */
    private Pool<Jedis>[] jedisSlavePools;
    /**
     * 是否只从 Redis 从节点读取数据
     */
    private boolean readFromSlave;
    /**
     * Redis 从节点的权重
     */
    private int[] slaveReadWeights;

    public Pool<Jedis> getJedisPool() {
        return jedisPool;
    }

    public void setJedisPool(Pool<Jedis> jedisPool) {
        this.jedisPool = jedisPool;
    }

    public Pool<Jedis>[] getJedisSlavePools() {
        return jedisSlavePools;
    }

    public void setJedisSlavePools(Pool<Jedis>... jedisSlavePools) {
        this.jedisSlavePools = jedisSlavePools;
    }

    public boolean isReadFromSlave() {
        return readFromSlave;
    }

    public void setReadFromSlave(boolean readFromSlave) {
        this.readFromSlave = readFromSlave;
    }

    public int[] getSlaveReadWeights() {
        return slaveReadWeights;
    }

    public void setSlaveReadWeights(int... slaveReadWeights) {
        this.slaveReadWeights = slaveReadWeights;
    }
}
