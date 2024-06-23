package com.gzy.custom.ttlthreadlocal.ttl;

import com.gzy.custom.ttlthreadlocal.Spi.TtlEnhanced;
import com.gzy.custom.ttlthreadlocal.Spi.TtlWrapper;

import java.util.concurrent.atomic.AtomicReference;


public final class TtlRunnable implements Runnable, TtlWrapper<Runnable>, TtlEnhanced {

    private final AtomicReference<Object> capturedRef;
    private final Runnable runnable;
    private final boolean releaseTtlValueReferenceAfterRun;

    private TtlRunnable( Runnable runnable, boolean releaseTtlValueReferenceAfterRun) {
        this.capturedRef = new AtomicReference<>(TransmittableThreadLocal.Transmitter.capture());
        this.runnable = runnable;
        this.releaseTtlValueReferenceAfterRun = releaseTtlValueReferenceAfterRun;
    }

    public static TtlRunnable get(Runnable runnable, boolean releaseTtlValueReferenceAfterRun) {
        return get(runnable, releaseTtlValueReferenceAfterRun, false);
    }
    
    /**
     * 包装runnable，默认线程执行完，不释放从父线程捕获的threadlocal值
     * @param runnable
     * @return
     */
    public static TtlRunnable get(Runnable runnable) {
        return get(runnable, false, false);
    }

    public static TtlRunnable get(Runnable runnable, boolean releaseTtlValueReferenceAfterRun, boolean idempotent) {
        if (runnable == null)
            return null;

        if (runnable instanceof TtlEnhanced) {
            // 避免重复包装，并且确定是包装类
            if (idempotent)
                return (TtlRunnable)runnable;
            else
                throw new IllegalStateException("Already TtlRunnable!");
        }
        return new TtlRunnable(runnable, releaseTtlValueReferenceAfterRun);
    }

    public Runnable getRunnable() {
        return unWrap();
    }

    @Override
    public Runnable unWrap() {
        return runnable;
    }

    public static Runnable unwrap(Runnable runnable) {
        if (!(runnable instanceof TtlRunnable))
            return runnable;
        else
            return ((TtlRunnable)runnable).getRunnable();
    }
    
    @Override
    public void run() {
        Object capture = capturedRef.get();
        // 使用AtomicReference cas保证多线程并发，同时调用ttlRunnable 执行同一个线程，保证线程只会被执行一次
        if (capture == null || (releaseTtlValueReferenceAfterRun && capturedRef.compareAndSet(capture, null))) {
            throw new IllegalStateException("TTL value reference is released after run!");
        }
        final Object backup = TransmittableThreadLocal.Transmitter.replay(capture);
        try {
            runnable.run();
        } finally {
            TransmittableThreadLocal.Transmitter.restore(backup);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TtlRunnable that = (TtlRunnable) o;

        return runnable.equals(that.runnable);
    }

    @Override
    public int hashCode() {
        return runnable.hashCode();
    }

    @Override
    public String toString() {
        return this.getClass().getName() + " - " + runnable.toString();
    }

}
