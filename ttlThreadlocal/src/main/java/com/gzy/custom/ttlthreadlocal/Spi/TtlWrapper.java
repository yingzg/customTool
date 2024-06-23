package com.gzy.custom.ttlthreadlocal.Spi;

public interface TtlWrapper<T> extends TtlEnhanced {

    T unWrap();

}
