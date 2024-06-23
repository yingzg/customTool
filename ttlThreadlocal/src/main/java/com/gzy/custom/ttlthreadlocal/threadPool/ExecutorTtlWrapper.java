package com.gzy.custom.ttlthreadlocal.threadPool;

import com.gzy.custom.ttlthreadlocal.Spi.TtlEnhanced;
import com.gzy.custom.ttlthreadlocal.Spi.TtlWrapper;
import com.gzy.custom.ttlthreadlocal.ttl.TtlRunnable;

import java.util.concurrent.Executor;

public class ExecutorTtlWrapper implements Executor, TtlWrapper<Executor>, TtlEnhanced {
    private final Executor executor;
    protected final boolean idempotent;

    ExecutorTtlWrapper(Executor executor, boolean idempotent) {
        this.executor = executor;
        this.idempotent = idempotent;
    }

    @Override
    public void execute(Runnable command) {
        executor.execute(TtlRunnable.get(command, false, idempotent));
    }

    @Override
    public Executor unWrap() {
        return executor;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ExecutorTtlWrapper that = (ExecutorTtlWrapper) o;

        return executor.equals(that.executor);
    }

    @Override
    public int hashCode() {
        return executor.hashCode();
    }

    @Override
    public String toString() {
        return this.getClass().getName() + " - " + executor;
    }
}
