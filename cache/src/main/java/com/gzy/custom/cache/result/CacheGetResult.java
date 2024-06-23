package com.gzy.custom.cache.result;

import com.gzy.custom.cache.CacheValueHolder;
import com.gzy.custom.cache.ResultData;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class CacheGetResult<V> extends CacheResult {

    private V value;
    private CacheValueHolder<V> holder;

    public static final CacheGetResult NOT_EXISTS_WITHOUT_MSG = new CacheGetResult(CacheResultCode.NOT_EXISTS, null, null);
    public static final CacheGetResult EXPIRED_WITHOUT_MSG = new CacheGetResult(CacheResultCode.EXPIRED, null ,null);


    public CacheGetResult(CacheResultCode resultCode, String message, CacheValueHolder<V> holder) {
        super(CompletableFuture.completedFuture(new ResultData(resultCode, message, holder)));
    }

    public CacheGetResult(CompletionStage<ResultData> future) {
        super(future);
    }

    public CacheGetResult(Throwable ex) {
        super(ex);
    }

    public V getValue() {
        waitForResult();
        return value;
    }

    @Override
    public void fetchResultSuccess(ResultData resultData) {
        super.fetchResultSuccess(resultData);
        holder = (CacheValueHolder<V>) resultData.getOriginData();
        value = (V) unwrapValue(holder);
    }

    /**
     * 缓存包装类，可以递归层层包装，unwrapValue方法会拆除所有的包装，获取原始缓存值
     * @param holder
     * @return
     */
   public static Object unwrapValue(Object holder) {
        Object v = holder;
        while (v != null && v instanceof CacheValueHolder) {
            v = ((CacheValueHolder) v).getValue();
        }
        return v;
    }

    @Override
    public void fetchResultFail(Throwable e) {
        super.fetchResultFail(e);
        value = null;
    }

    public CacheValueHolder<V> getHolder() {
        waitForResult();
        return holder;
    }

}



