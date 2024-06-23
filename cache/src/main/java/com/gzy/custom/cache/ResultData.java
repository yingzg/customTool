package com.gzy.custom.cache;

import com.gzy.custom.cache.result.CacheGetResult;
import com.gzy.custom.cache.result.CacheResultCode;

public class ResultData {

    private CacheResultCode resultCode;
    private String message;
    private Object data;

    public ResultData(Throwable e) {
        this.resultCode = CacheResultCode.FAIL;
        this.message = "Ex : " + e.getClass() + ", " + e.getMessage();
    }

    public ResultData(CacheResultCode resultCode, String message, Object data) {
        this.resultCode = resultCode;
        this.message = message;
        this.data = data;
    }

    /**
     * 获取缓存结果值（有包装缓存的，会拆包装，获取原始的缓存值）
     * @return
     */
    public Object getData() {
        return CacheGetResult.unwrapValue(data);
    }

    /**
     * 获取缓存结果值
     * @return
     */
    public Object getOriginData() {
        return data;
    }


    public CacheResultCode getResultCode() {
        return resultCode;
    }

    public void setResultCode(CacheResultCode resultCode) {
        this.resultCode = resultCode;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public void setData(Object data) {
        this.data = data;
    }

}
