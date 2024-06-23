package com.gzy.custom.ttlthreadlocal.ttl;

import com.gzy.custom.ttlthreadlocal.Spi.TtlEnhanced;
import com.gzy.custom.ttlthreadlocal.Spi.TtlWrapper;

import java.util.function.Supplier;

/**
 * 工具方法包装类
 */
public final class TtlWrappers {

    public static <T> Supplier<T> wrapSupplier(Supplier<T> supplier) {
        if (supplier == null)
            return null;
        if (supplier instanceof TtlEnhanced)
            return supplier;
        else {
            return new TtlSupplier<>(supplier);
        }
    }

    private static class TtlSupplier<T> implements Supplier<T>, TtlWrapper<Supplier<T>> {
        final Supplier<T> supplier;
        final Object capture;

        public TtlSupplier(Supplier<T> supplier) {
            this.supplier = supplier;
            this.capture = TransmittableThreadLocal.Transmitter.capture();
        }

        @Override
        public Supplier<T> unWrap() {
            return supplier;
        }

        @Override
        public T get() {
            final Object backup = TransmittableThreadLocal.Transmitter.replay(capture);
            try {
                return supplier.get();
            } finally {
                TransmittableThreadLocal.Transmitter.restore(backup);
            }
        }

        @Override
        public int hashCode() {
            return supplier.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            TtlSupplier<?> that = (TtlSupplier<?>)o;
            return supplier.equals(that.supplier);
        }

        @Override
        public String toString() {
            return this.getClass().getName() + " - " + supplier.toString();
        }
    }


}
