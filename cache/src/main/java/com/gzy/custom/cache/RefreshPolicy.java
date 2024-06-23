package com.gzy.custom.cache;

import java.util.concurrent.TimeUnit;

public class RefreshPolicy implements Cloneable {

    /**
     * 刷新间隔
     */
    private long refreshMillis;
    /**
     * 多长时间没访问则停止刷新
     */
    private long stopRefreshAfterLastAccessMillis;
    /**
     * 刷新锁的超时时间
     */
    private long refreshLockTimeoutMillis = 60 * 1000;

    public static RefreshPolicy newPolicy(long time, TimeUnit timeUnit) {
        RefreshPolicy p = new RefreshPolicy();
        p.refreshMillis = timeUnit.toMillis(time);
        return p;
    }

    public RefreshPolicy stopRefreshAfterLastAccess(long time, TimeUnit timeUnit) {
        this.stopRefreshAfterLastAccessMillis = timeUnit.toMillis(time);
        return this;
    }

    public RefreshPolicy refreshLockTimeout(long time, TimeUnit timeUnit) {
        this.refreshLockTimeoutMillis = timeUnit.toMillis(time);
        return this;
    }

    @Override
    public RefreshPolicy clone() {
        try {
            return (RefreshPolicy) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    public long getRefreshMillis() {
        return refreshMillis;
    }

    public void setRefreshMillis(long refreshMillis) {
        this.refreshMillis = refreshMillis;
    }

    public long getStopRefreshAfterLastAccessMillis() {
        return stopRefreshAfterLastAccessMillis;
    }

    public void setStopRefreshAfterLastAccessMillis(long stopRefreshAfterLastAccessMillis) {
        this.stopRefreshAfterLastAccessMillis = stopRefreshAfterLastAccessMillis;
    }

    public long getRefreshLockTimeoutMillis() {
        return refreshLockTimeoutMillis;
    }

    public void setRefreshLockTimeoutMillis(long refreshLockTimeoutMillis) {
        this.refreshLockTimeoutMillis = refreshLockTimeoutMillis;
    }

}
