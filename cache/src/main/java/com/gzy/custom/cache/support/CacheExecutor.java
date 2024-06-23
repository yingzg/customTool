package com.gzy.custom.cache.support;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

public class CacheExecutor {
    protected static ScheduledExecutorService defaultExecutor;
    protected static ScheduledExecutorService heavyIOExecutor;

    private static int threadCount;

    static {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                if (defaultExecutor != null) {
                    defaultExecutor.shutdownNow();
                }
                if (heavyIOExecutor != null) {
                    heavyIOExecutor.shutdownNow();
                }
            }
        });
    }

    public static ScheduledExecutorService defaultExecutor() {
        if (defaultExecutor != null) {
            return defaultExecutor;
        }
        synchronized (CacheExecutor.class) {
            if (defaultExecutor == null) {
                ThreadFactory tf = r -> {
                    Thread t = new Thread(r, "JetCacheDefaultExecutor");
                    t.setDaemon(true);
                    return t;
                };
                defaultExecutor = new ScheduledThreadPoolExecutor(1, tf, new ThreadPoolExecutor.DiscardPolicy());
            }
        }
        return defaultExecutor;
    }

    public static ScheduledExecutorService heavyIOExecutor() {
        if (heavyIOExecutor != null) {
            return heavyIOExecutor;
        }
        synchronized (CacheExecutor.class) {
            if (heavyIOExecutor == null) {
                ThreadFactory tf = r -> {
                    Thread t = new Thread(r, "JetCacheHeavyIOExecutor" + threadCount++);
                    t.setDaemon(true);
                    return t;
                };
                heavyIOExecutor = new ScheduledThreadPoolExecutor(10, tf, new ThreadPoolExecutor.DiscardPolicy());
            }
        }
        return heavyIOExecutor;
    }

    public static void setDefaultExecutor(ScheduledExecutorService executor) {
        CacheExecutor.defaultExecutor = executor;
    }

    public static void setHeavyIOExecutor(ScheduledExecutorService heavyIOExecutor) {
        CacheExecutor.heavyIOExecutor = heavyIOExecutor;
    }
}
