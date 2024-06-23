package com.gzy.custom.ttlagent.agent.helper;

import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.logging.Level;
import java.util.logging.Logger;

import javassist.CtBehavior;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.NotFoundException;

/**
 * 字节码增强工具类
 */
public final class TtlTransformletHelper {
    private static final Logger logger = Logger.getLogger(TtlTransformletHelper.class.getName());

    public static URL getLocationUrlOfClass(CtClass clazz) {
        try {
            // proxy classes is dynamic, no class file
            if (clazz.getName().startsWith("com.sun.proxy."))
                return null;

            return clazz.getURL();
        } catch (Exception e) {
            logger.log(Level.WARNING, "Fail to getLocationUrlOfClass " + clazz.getName() + ", cause: " + e.toString());
            return null;
        }
    }

    public static String getLocationFileOfClass(CtClass clazz) {
        final URL location = getLocationUrlOfClass(clazz);
        if (location == null)
            return null;

        return location.getFile();
    }

    public static boolean isClassUnderPackage(String className, String packageName) {
        String packageOfClass = getPackageName(className);
        return packageOfClass.equals(packageName) || packageOfClass.startsWith(packageName + ".");
    }

    public static String getPackageName(String className) {
        final int idx = className.lastIndexOf('.');
        if (-1 == idx)
            return "";

        return className.substring(0, idx);
    }

    public static String signatureOfMethod(final CtBehavior method) throws NotFoundException {
        final StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append(Modifier.toString(method.getModifiers()));
        if (method instanceof CtMethod) {
            final String returnType = ((CtMethod)method).getReturnType().getSimpleName();
            stringBuilder.append(" ").append(returnType);
        }
        stringBuilder.append(" ").append(method.getName()).append("(");

        final CtClass[] parameterTypes = method.getParameterTypes();
        for (int i = 0; i < parameterTypes.length; i++) {
            CtClass parameterType = parameterTypes[i];
            if (i != 0)
                stringBuilder.append(", ");
            stringBuilder.append(parameterType.getSimpleName());
        }

        stringBuilder.append(")");
        return stringBuilder.toString();
    }

}
