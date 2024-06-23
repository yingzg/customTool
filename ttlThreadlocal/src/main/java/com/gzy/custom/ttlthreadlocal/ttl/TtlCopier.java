package com.gzy.custom.ttlthreadlocal.ttl;

@FunctionalInterface
public interface TtlCopier<T> {

    T copy(T parentValue);

}
