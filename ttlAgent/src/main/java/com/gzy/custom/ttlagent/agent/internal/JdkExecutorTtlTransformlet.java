package com.gzy.custom.ttlagent.agent.internal;

import com.gzy.custom.ttlagent.agent.TtlAgent;
import com.gzy.custom.ttlagent.agent.TtlTransformlet;
import com.gzy.custom.ttlagent.agent.helper.AbstractExecutorTtlTransformlet;

import java.util.HashSet;
import java.util.Set;


public final class JdkExecutorTtlTransformlet extends AbstractExecutorTtlTransformlet implements TtlTransformlet {

    private static Set<String> getExecutorClassNames() {
        Set<String> executorClassNames = new HashSet<>();

        executorClassNames.add(THREAD_POOL_EXECUTOR_CLASS_NAME);
        executorClassNames.add("java.util.concurrent.ScheduledThreadPoolExecutor");

        return executorClassNames;
    }

    public JdkExecutorTtlTransformlet() {
        super(getExecutorClassNames(), TtlAgent.isDisableInheritableForThreadPool());
    }
}
