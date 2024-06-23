package com.gzy.custom.cache.result;

import com.gzy.custom.cache.ResultData;

import java.time.Duration;
import java.util.concurrent.*;

public class CacheResult {

    public static final String MSG_ILLEGAL_ARGUMENT = "illegal argument";
    public static final CacheResult FAIL_ILLEGAL_ARGUMENT = new CacheResult(CacheResultCode.FAIL, MSG_ILLEGAL_ARGUMENT);

    public static final CacheResult SUCCESS_WITHOUT_MSG = new CacheResult(CacheResultCode.SUCCESS, null);
    public static final CacheResult PART_SUCCESS_WITHOUT_MSG = new CacheResult(CacheResultCode.PART_SUCCESS, null);
    public static final CacheResult FAIL_WITHOUT_MSG = new CacheResult(CacheResultCode.FAIL, null);

    public static final CacheResult EXISTS_WITHOUT_MSG = new CacheResult(CacheResultCode.EXISTS, null);

    static Duration ASYNC_RESULT_TIMEOUT = Duration.ofMillis(1000);
    private static Duration DEFAULT_TIMEOUT = ASYNC_RESULT_TIMEOUT;
    private CacheResultCode resultCode;
    private String message;
    private CompletionStage<ResultData> future;

    private Duration timeout = DEFAULT_TIMEOUT;

    public CacheResult(CompletionStage<ResultData> future) {
        this.future = future;
    }

    public CacheResult(CacheResultCode resultCode, String message) {
        this(CompletableFuture.completedFuture(new ResultData(resultCode, message, null)));
    }

    public CacheResult(Throwable ex) {
        future = CompletableFuture.completedFuture(new ResultData(ex));
    }

    public boolean isSuccess() {
        return getResultCode() == CacheResultCode.SUCCESS;
    }

    public CacheResultCode getResultCode() {
        waitForResult();
        return resultCode;
    }

    public String getMessage() {
        waitForResult();
        return message;
    }

    public void waitForResult() {
        waitForResult(timeout);
    }

    public void waitForResult(Duration timeout) {
        if (resultCode != null) {
            return;
        }
        try {
            ResultData resultData = future.toCompletableFuture().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            fetchResultSuccess(resultData);
        } catch (TimeoutException | InterruptedException | ExecutionException e) {
            fetchResultFail(e);
        }
    }

    public void fetchResultSuccess(ResultData resultData) {
        resultCode = resultData.getResultCode();
        message = resultData.getMessage();
    }

    public void fetchResultFail(Throwable e) {
        resultCode = CacheResultCode.FAIL;
        message = e.getClass() + ":" + e.getMessage();
    }

    public CompletionStage<ResultData> future() {
        return future;
    }

    public static void setDefaultTimeout(Duration defaultTimeout) {
        DEFAULT_TIMEOUT = defaultTimeout;
    }

    public void setTimeout(Duration timeout) {
        this.timeout = timeout;
    }

}
