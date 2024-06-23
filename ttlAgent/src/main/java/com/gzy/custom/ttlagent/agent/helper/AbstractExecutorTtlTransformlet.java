package com.gzy.custom.ttlagent.agent.helper;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;


import com.gzy.custom.ttlagent.agent.ClassInfo;
import com.gzy.custom.ttlagent.agent.TtlTransformlet;
import javassist.*;

import static com.gzy.custom.ttlagent.agent.helper.TtlTransformletHelper.signatureOfMethod;

public abstract class AbstractExecutorTtlTransformlet implements TtlTransformlet {
    protected static final String RUNNABLE_CLASS_NAME = "java.lang.Runnable";
    protected static final String TTL_RUNNABLE_CLASS_NAME = "com.alibaba.ttl.TtlRunnable";
    protected static final String THREAD_FACTORY_CLASS_NAME = "java.util.concurrent.ThreadFactory";
    protected static final String THREAD_POOL_EXECUTOR_CLASS_NAME = "java.util.concurrent.ThreadPoolExecutor";

    protected final Logger logger = Logger.getLogger(getClass().getName());

    protected final Set<String> executorClassNames;
    protected final boolean disableInheritableForThreadPool;

    private final Map<String, String> paramTypeNameToDecorateMethodClass = new HashMap<>();

    public AbstractExecutorTtlTransformlet(Set<String> executorClassNames, boolean disableInheritableForThreadPool) {
        this.executorClassNames = Collections.unmodifiableSet(executorClassNames);
        this.disableInheritableForThreadPool = disableInheritableForThreadPool;

        paramTypeNameToDecorateMethodClass.put(RUNNABLE_CLASS_NAME, TTL_RUNNABLE_CLASS_NAME);
    }

    @Override
    public void doTransform(ClassInfo classInfo) throws IOException, NotFoundException, CannotCompileException {

        /*  if (isClassAtPackageJavaUtil(classInfo.getClassName())) return;*/
        final CtClass clazz = classInfo.getCtClass();
        if (executorClassNames.contains(classInfo.getClassName())) {
            for (CtMethod method : clazz.getDeclaredMethods()) {
                updateSubmitMethodsOfExecutorClass_decorateToTtlWrapperAndSetAutoWrapperAttachment(method);
            }
            if (disableInheritableForThreadPool)
                updateConstructorDisableInheritable(clazz);

            classInfo.setModified();
        }
    }

    private void updateSubmitMethodsOfExecutorClass_decorateToTtlWrapperAndSetAutoWrapperAttachment(
        final CtMethod method) throws NotFoundException, CannotCompileException {
        final int modifiers = method.getModifiers();
        if (!(Modifier.isPublic(modifiers) || Modifier.isStatic(modifiers))) {
            return;
        }
        CtClass[] parameterTypes = method.getParameterTypes();
        StringBuilder insertCode = new StringBuilder();
        for (int i = 0; i < parameterTypes.length; i++) {
            final String paramTypeName = parameterTypes[i].getName();
            if (paramTypeNameToDecorateMethodClass.containsKey(paramTypeName)) {

                // $3 = com.alibaba.ttl.threadpool.agent.transformlet.helper.TtlTransformletHelper.doAutoWrap($3)
                String code = String.format(
                    // auto decorate to TTL wrapper
                    "$%d = com.alibaba.ttl.threadpool.agent.transformlet.helper.TtlTransformletHelper.doAutoWrap($%<d);",
                    i + 1);
                logger.info("insert code before method " + signatureOfMethod(method) + " of class "
                    + method.getDeclaringClass().getName() + ":\n" + code);
                insertCode.append(code);
            }
        }
        if (insertCode.length() > 0) {
            logger.info("insert code before method " + signatureOfMethod(method) + " of class "
                + method.getDeclaringClass().getName() + ":\n" + insertCode);
            method.insertBefore(insertCode.toString());
        }
    }

    private void updateConstructorDisableInheritable(final CtClass clazz)
        throws NotFoundException, CannotCompileException {
        for (CtConstructor constructor : clazz.getDeclaredConstructors()) {
            final CtClass[] parameterTypes = constructor.getParameterTypes();
            final StringBuilder insertCode = new StringBuilder();
            for (int i = 0; i < parameterTypes.length; i++) {
                final String paramTypeName = parameterTypes[i].getName();
                if (THREAD_FACTORY_CLASS_NAME.equals(paramTypeName)) {
                    String code = String.format(
                        "$%d = com.alibaba.ttl.threadpool.TtlExecutors.getDisableInheritableThreadFactory($%<d);",
                        i + 1);
                    insertCode.append(code);
                }
            }
            if (insertCode.length() > 0) {
                logger.info("insert code before constructor " + signatureOfMethod(constructor) + " of class "
                    + constructor.getDeclaringClass().getName() + ": " + insertCode);
                constructor.insertBefore(insertCode.toString());
            }
        }
    }

}
