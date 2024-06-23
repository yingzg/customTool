package com.gzy.custom.ttlthreadlocal.threadPool;

import com.gzy.custom.ttlthreadlocal.Spi.TtlWrapper;

import java.util.concurrent.ThreadFactory;


public interface DisableInheritableThreadFactory extends ThreadFactory, TtlWrapper<ThreadFactory> {

    @Override
    ThreadFactory unWrap();
}
