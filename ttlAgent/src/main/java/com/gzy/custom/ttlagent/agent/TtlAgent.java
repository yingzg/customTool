package com.gzy.custom.ttlagent.agent;

import com.gzy.custom.ttlagent.agent.internal.JdkExecutorTtlTransformlet;

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;


public final class TtlAgent {

    private static final boolean isDisableInheritableForThreadPool = true;
    private static final boolean isLogClassTransform = true;
    private static volatile boolean ttlAgentLoaded = false;

    public static void premain(final String agentArgs, final Instrumentation inst) {
        final Logger logger = Logger.getLogger(TtlAgent.class.getName());
        try {
            final List<TtlTransformlet> transformletList = new ArrayList<>();

            transformletList.add(new JdkExecutorTtlTransformlet());
            final ClassFileTransformer transformer = new TtlTransformer(transformletList, isLogClassTransform());
            inst.addTransformer(transformer, true);
            logger.info("[TtlAgent.premain] add Transformer " + transformer.getClass().getName() + " success");

            logger.info("[TtlAgent.premain] end");

            ttlAgentLoaded = true;

        } catch (Exception e) {
            String msg = "Fail to load TtlAgent , cause: " + e.toString();
            logger.log(Level.SEVERE, msg, e);
            throw new IllegalStateException(msg, e);
        }
    }

    public static boolean isTtlAgentLoaded() {
        return ttlAgentLoaded;
    }

    public static boolean isDisableInheritableForThreadPool() {
        return isDisableInheritableForThreadPool;
    }

    public static boolean isLogClassTransform() {
        return isLogClassTransform;
    }

}
