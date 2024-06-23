package com.gzy.custom.ttlagent.agent;

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.gzy.custom.ttlagent.agent.helper.TtlTransformletHelper.isClassUnderPackage;

public class TtlTransformer implements ClassFileTransformer {

    private static final byte[] NO_TRANSFORM = null;
    private final List<TtlTransformlet> transformletList = new ArrayList<>();
    private final boolean logClassTransform;

    private final Logger logger = Logger.getLogger(TtlTransformer.class.getName());

    TtlTransformer(List<? extends TtlTransformlet> transformletList, boolean logClassTransform) {

        this.logClassTransform = logClassTransform;
        for (TtlTransformlet ttlTransformlet : transformletList) {
            this.transformletList.add(ttlTransformlet);
            logger.info("[TtlTransformer] add Transformlet " + ttlTransformlet.getClass().getName());
        }
    }

    @Override
    public byte[] transform(ClassLoader loader, String classFile, Class<?> classBeingRedefined,
        ProtectionDomain protectionDomain, byte[] classFileBuffer) throws IllegalClassFormatException {

        try {
            // Lambda has no class file, no need to transform, just return.
            if (classFile == null)
                return NO_TRANSFORM;

            final ClassInfo classInfo = new ClassInfo(classFile, classFileBuffer, loader);
            if (isClassUnderPackage(classInfo.getClassName(), "com.alibaba.ttl"))
                return NO_TRANSFORM;
            if (isClassUnderPackage(classInfo.getClassName(), "java.lang"))
                return NO_TRANSFORM;

            if (logClassTransform)
                logger.info("[TtlTransformer] transforming " + classInfo.getClassName() + " from classloader "
                    + classInfo.getClassLoader() + " at location " + classInfo.getLocationUrl());

            for (TtlTransformlet transformlet : transformletList) {
                transformlet.doTransform(classInfo);
                if (classInfo.isModified()) {
                    logger.info("[TtlTransformer] " + transformlet.getClass().getName() + " transformed "
                        + classInfo.getClassName() + " from classloader " + classInfo.getClassLoader() + " at location "
                        + classInfo.getLocationUrl());
                    return classInfo.getCtClass().toBytecode();
                }
            }
        } catch (Throwable t) {
            String msg = "[TtlTransformer] fail to transform class " + classFile + ", cause: " + t.toString();
            logger.log(Level.SEVERE, msg, t);
            throw new IllegalStateException(msg, t);
        }

        return NO_TRANSFORM;
    }

}
