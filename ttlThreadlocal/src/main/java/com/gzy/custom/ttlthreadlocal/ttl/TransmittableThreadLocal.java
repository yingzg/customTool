package com.gzy.custom.ttlthreadlocal.ttl;


import java.util.Iterator;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;


public class TransmittableThreadLocal<T> extends InheritableThreadLocal<T> implements TtlCopier<T> {

        private static final Logger logger = Logger.getLogger(TransmittableThreadLocal.class.getName());

    // 是否禁用忽略NULL值的语义
    private final boolean disableIgnoreNullValueSemantics;

    // 默认是false，也就是不禁用忽略NULL值的语义，
    // 也就是忽略NULL值，也就是默认的话，NULL值传入不会覆盖原来已经存在的值
    public TransmittableThreadLocal() {
        this(false);
    }

    // 可以通过手动设置，去覆盖IgnoreNullValue的语义，
    // 如果设置为true，则是支持NULL值的设置，设置为true的时候，与ThreadLocal的语义一致
    public TransmittableThreadLocal(boolean disableIgnoreNullValueSemantics) {
        this.disableIgnoreNullValueSemantics = disableIgnoreNullValueSemantics;
    }

    // 模板方法，子类实现，在TtlRunnable或者TtlCallable执行前回调
    protected void beforeExecute() {}

    // 模板方法，子类实现，在TtlRunnable或者TtlCallable执行后回调
    protected void afterExecute() {}

    @Override
    public T get() {
        T value = super.get();
        // 允许设置null值||value不为空的时候
        if (disableIgnoreNullValueSemantics || value != null) {
            addThisToHolder();
        }
        return value;
    }

    @Override
    public void set(T value) {
        // 忽略null值 并且 value==null时 移除null值
        if (!disableIgnoreNullValueSemantics && value == null) {
            remove();
        } else {
            super.set(value);
            addThisToHolder();
        }
    }

    @Override
    public void remove() {
        super.remove();
        removeThisFromHolder();
    }

    private void superRemove() {
        super.remove();
    }
    
    private void addThisToHolder() {
        if (!holder.get().containsKey(this)) {
            holder.get().put((TransmittableThreadLocal<Object>)this, null);
        }
    }

    private void removeThisFromHolder() {
        if (holder.get().containsKey(this)) {
            holder.get().remove(this);
        }
    }

    /**
     * 执行目标方法，isBefore决定回调beforeExecute还是afterExecute，
     * 注意此回调方法会吞掉所有的异常只打印日志
     * @param isBefore
     */
    private static void doExecuteCallBack(boolean isBefore) {
        for (TransmittableThreadLocal<Object> threadLocal : holder.get().keySet()) {
            try {
                if (isBefore)
                    threadLocal.beforeExecute();
                else
                    threadLocal.afterExecute();
            } catch (Throwable t) {
                if (logger.isLoggable(Level.WARNING)) {
                    logger.log(Level.WARNING, "TTL exception when " + (isBefore ? "beforeExecute" : "afterExecute")
                        + ", cause: " + t.toString(), t);
                }
            }
        }
    }

    private static final InheritableThreadLocal<
        WeakHashMap<TransmittableThreadLocal<Object>, TtlCopier<Object>>> holder =
            new InheritableThreadLocal<WeakHashMap<TransmittableThreadLocal<Object>, TtlCopier<Object>>>() {
                @Override
                protected WeakHashMap<TransmittableThreadLocal<Object>, TtlCopier<Object>> initialValue() {
                    return new WeakHashMap<>();
                }

                @Override
                protected WeakHashMap<TransmittableThreadLocal<Object>, TtlCopier<Object>>
                    childValue(WeakHashMap<TransmittableThreadLocal<Object>, TtlCopier<Object>> parentValue) {
                    return new WeakHashMap<>(parentValue);
                }
            };

    @Override
    public T copy(T parentValue) {
        return parentValue;
    }

    private T copyValue() {
        return copy(get());
    }

    public static class Transmitter {

        /**
         * 手动注册thredlocal 全局保存类
         */
        private static volatile WeakHashMap<ThreadLocal<Object>, TtlCopier<Object>> threadLoadHolders =
            new WeakHashMap<>();

        // 标记WeakHashMap中的ThreadLocal的对应值为NULL的属性，便于后面清理
        private static final Object threadLocalClearMark = new Object();
        
        /**
         * 工具类，靠静态方法调用，禁止实例化
         */
        private Transmitter() {
            throw new InstantiationError("Must not instantiate this class");
        }

        public static Object capture() {
            return new SnapShot(captureTtlValue(), captureThreadLocalValue());
        }

        public static Object replay(Object capture) {
            SnapShot captureSnapShot = (SnapShot)capture;
            return new SnapShot(replayTtlValue(captureSnapShot.ttl2Value),
                replayThreadLocalValue(captureSnapShot.threadLocal2Value));
        }

        public static void restore(Object backup) {
            SnapShot backupSnapshot = (SnapShot)backup;
            restoreTtlValues(backupSnapshot.ttl2Value);
            restoreThreadLocalValues(backupSnapshot.threadLocal2Value);
        }

        public static Object clear() {
            return new SnapShot(clearTtlValues(), clearThreadLocalValues());
        }

        private static WeakHashMap<ThreadLocal<Object>, Object> clearThreadLocalValues() {
            final WeakHashMap<ThreadLocal<Object>, Object> threadLocal2Value =
                new WeakHashMap<>(threadLoadHolders.size());
            for (Map.Entry<ThreadLocal<Object>, TtlCopier<Object>> entry : threadLoadHolders.entrySet()) {
                final ThreadLocal<Object> threadLocal = entry.getKey();
                threadLocal2Value.put(threadLocal, threadLocalClearMark);
            }
            return replayThreadLocalValue(threadLocal2Value);
        }

        private static WeakHashMap<TransmittableThreadLocal<Object>, Object> clearTtlValues() {
            return replayTtlValue(new WeakHashMap<>(0));
        }
        
        private static void restoreTtlValues(WeakHashMap<TransmittableThreadLocal<Object>, Object> backup) {
            // 回调模板方法afterExecute
            doExecuteCallBack(false);
            // 这里的循环针对的是子线程，用于获取的是子线程的所有线程本地变量
            for (final Iterator<TransmittableThreadLocal<Object>> iterator = holder.get().keySet().iterator();
                iterator.hasNext();) {
                TransmittableThreadLocal<Object> threadLocal = iterator.next();
                // 如果子线程原来就绑定的线程本地变量的值，如果不包含某个父线程传来的对象，那么就删除
                // 这一步可以结合前面reply操作里面的方法段一起思考，如果不删除的话，就相当于子线程的原来存在的线程本地变量绑定值被父线程对应的值污染了
                if (!backup.containsKey(threadLocal)) {
                    iterator.remove();
                    threadLocal.superRemove();
                }
            }
            // 重新设置TTL的值到捕获的快照中
            // 其实真实的意图是：把子线程的线程本地变量恢复到reply()的备份（前面的循环已经做了父线程捕获变量的判断），本质上，等于把holder中绑定于子线程本地变量的部分恢复到reply操作之前的状态
            setTtlValuesTo(backup);
        }

        // 恢复所有的手动注册的ThreadLocal的值
        private static void restoreThreadLocalValues(WeakHashMap<ThreadLocal<Object>, Object> backup) {
            for (Map.Entry<ThreadLocal<Object>, Object> entry : backup.entrySet()) {
                final ThreadLocal<Object> threadLocal = entry.getKey();
                threadLocal.set(entry.getValue());
            }
        }

        /**
         * 重放操作，在子线程执行
         * @param ttlCapture
         * @return
         */
        private static WeakHashMap<TransmittableThreadLocal<Object>, Object>
            replayTtlValue(WeakHashMap<TransmittableThreadLocal<Object>, Object> ttlCapture) {
            WeakHashMap<TransmittableThreadLocal<Object>, Object> backup = new WeakHashMap<>();
            // 清理所有的非捕获快照中的TTL变量，以防有中间过程引入的额外的TTL变量（除了父线程的本地变量）影响了任务执行后的重放操作
            // 简单来说就是：移除所有子线程的不包含在父线程捕获的线程本地变量集合的中所有子线程本地变量和对应的值
            /**
             * 这个问题可以举个简单的例子： static TransmittableThreadLocal<Integer> TTL = new TransmittableThreadLocal<>();
             *
             * 线程池中的子线程C中原来初始化的时候，在线程C中绑定了TTL的值为10087，C线程是核心线程不会主动销毁。
             *
             * 父线程P在没有设置TTL值的前提下，调用了线程C去执行任务，那么在C线程的Runnable包装类中通过TTL#get()就会获取到10087，显然是不符合预期的
             *
             * 所以，在C线程的Runnable包装类之前之前，要从C线程的线程本地变量，移除掉不包含在父线程P中的所有线程本地变量，确保Runnable包装类执行期间只能拿到父线程中捕获到的线程本地变量
             */
            for (Iterator iterator = holder.get().keySet().iterator(); iterator.hasNext();) {
                TransmittableThreadLocal<Object> threadLocal = (TransmittableThreadLocal<Object>)iterator.next();
                backup.put(threadLocal, threadLocal.get());
                if (!ttlCapture.containsKey(threadLocal)) {
                    iterator.remove();
                    threadLocal.superRemove();
                }
            }
            // 重新设置TTL的值到捕获的快照中
            // 其实真实的意图是：把从父线程中捕获的所有线程本地变量重写设置到TTL中，本质上，子线程holder里面的TTL绑定的值会被刷新
            setTtlValuesTo(ttlCapture);

            // 回调模板方法beforeExecute
            doExecuteCallBack(true);
            return backup;
        }

        private static void setTtlValuesTo(WeakHashMap<TransmittableThreadLocal<Object>, Object> ttlValues) {
            for (Map.Entry<TransmittableThreadLocal<Object>, Object> entry : ttlValues.entrySet()) {
                TransmittableThreadLocal<Object> threadLocal = entry.getKey();
                // 重新设置TTL值，本质上，当前线程（子线程）holder里面的TTL绑定的值会被刷新
                threadLocal.set(entry.getValue());
            }
        }

        /**
         * 重放所有的手动注册的ThreadLocal的值
         * @param threadLocalCapture
         * @return
         */
        private static WeakHashMap<ThreadLocal<Object>, Object>
            replayThreadLocalValue(WeakHashMap<ThreadLocal<Object>, Object> threadLocalCapture) {
            WeakHashMap<ThreadLocal<Object>, Object> backup = new WeakHashMap<>();
            // 注意这里是遍历捕获的快照中的ThreadLocal
            for (Map.Entry<ThreadLocal<Object>, Object> entry : threadLocalCapture.entrySet()) {
                final ThreadLocal<Object> threadLocal = entry.getKey();
                // 添加到备份中
                backup.put(threadLocal, threadLocal.get());
                final Object value = entry.getValue();
                // 如果值为清除标记则绑定在当前线程的变量进行remove，否则设置值覆盖
                if (value == threadLocalClearMark)
                    threadLocal.remove();
                else
                    threadLocal.set(value);
            }
            return backup;
        }
        
        /**
         * 捕获父线程 TransmittableThreadLocal值
         * @return
         */
        private static WeakHashMap<TransmittableThreadLocal<Object>, Object> captureTtlValue() {
            WeakHashMap<TransmittableThreadLocal<Object>, Object> ttlValue = new WeakHashMap<>();
            for (Map.Entry<TransmittableThreadLocal<Object>, ?> entry : holder.get().entrySet()) {
                TransmittableThreadLocal<Object> threadLocal = entry.getKey();
                ttlValue.put(threadLocal, threadLocal.copyValue());
            }
            return ttlValue;
        }

        /**
         * 捕获手动注册的threadlocal值
         * 
         * @return
         */
        private static WeakHashMap<ThreadLocal<Object>, Object> captureThreadLocalValue() {
            WeakHashMap<ThreadLocal<Object>, Object> threadLocalValue = new WeakHashMap<>();
            for (Map.Entry<ThreadLocal<Object>, TtlCopier<Object>> entry : threadLoadHolders.entrySet()) {
                ThreadLocal<Object> threadLocal = entry.getKey();
                TtlCopier<Object> copier = entry.getValue();
                threadLocalValue.put(threadLocal, copier.copy(threadLocal.get()));
            }
            return threadLocalValue;
        }

        public interface Transmittee {
            public Object capture();

            public Object replay();

            public void restore();

            public void clear();

        }

    }

    public static class SnapShot {
        final WeakHashMap<TransmittableThreadLocal<Object>, Object> ttl2Value;
        final WeakHashMap<ThreadLocal<Object>, Object> threadLocal2Value;

        public SnapShot(WeakHashMap<TransmittableThreadLocal<Object>, Object> ttl2Value,
            WeakHashMap<ThreadLocal<Object>, Object> threadLocal2Value) {
            this.ttl2Value = ttl2Value;
            this.threadLocal2Value = threadLocal2Value;
        }
    }

}
