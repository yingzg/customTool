package com.gzy.custom.cache.config;

import com.gzy.custom.cache.support.DecoderMap;
import com.gzy.custom.cache.support.JavaValueEncoder;

import java.util.function.Function;


public class ExternalCacheConfig<K, V> extends CacheConfig<K, V> {
    /**
     * 指定缓存 Key 的前缀，也就是每个缓存实例的 [keyPrefix][area_]cacheName
     */
    private String keyPrefix;
    /**
     * 缓存数据编码函数，支持JAVA、Kryo
     */
    private Function<Object, byte[]> valueEncoder = JavaValueEncoder.INSTANCE;
    /**
     * 缓存数据解码函数
     */
    private Function<byte[], Object> valueDecoder = DecoderMap.defaultJavaValueDecoder();

    public String getKeyPrefix() {
        return keyPrefix;
    }

    public void setKeyPrefix(String keyPrefix) {
        this.keyPrefix = keyPrefix;
    }

    public Function<Object, byte[]> getValueEncoder() {
        return valueEncoder;
    }

    public void setValueEncoder(Function<Object, byte[]> valueEncoder) {
        this.valueEncoder = valueEncoder;
    }

    public Function<byte[], Object> getValueDecoder() {
        return valueDecoder;
    }

    public void setValueDecoder(Function<byte[], Object> valueDecoder) {
        this.valueDecoder = valueDecoder;
    }
}

