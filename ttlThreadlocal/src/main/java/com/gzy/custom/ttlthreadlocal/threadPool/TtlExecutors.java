package com.gzy.custom.ttlthreadlocal.threadPool;

import com.gzy.custom.ttlthreadlocal.Spi.TtlEnhanced;
import com.gzy.custom.ttlthreadlocal.Spi.TtlWrapper;

import java.util.concurrent.Executor;

public final class TtlExecutors {

    public static Executor getTtlExecutor(Executor executor) {
        if (executor == null || executor instanceof TtlEnhanced) {
            return executor;
        }
        return new ExecutorTtlWrapper(executor, true);
    }

    public static <T extends Executor> boolean isTtlWrapper(T executor) {
        return executor instanceof TtlWrapper;
    }

    public static <T extends Executor> T unwrap(T executor) {
        if (!isTtlWrapper(executor))
            return executor;
        return (T)((ExecutorTtlWrapper)executor).unWrap();
    }

}
