package com.gzy.custom.ttlagent.agent;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.LoaderClassPath;

import static com.gzy.custom.ttlagent.agent.helper.TtlTransformletHelper.getLocationUrlOfClass;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URL;



public class ClassInfo {

    /**
     * 字节码增加文件的路径
     */
    private final String transformerClassFile;
    private final String className;
    private final byte[] classFileBuffer;
    private final ClassLoader loader;

    private CtClass ctClass;

    public ClassInfo(String transformerClassFile, byte[] classFileBuffer, ClassLoader loader) {
        this.transformerClassFile = transformerClassFile;
        this.className = toClassName(transformerClassFile);
        this.classFileBuffer = classFileBuffer;
        this.loader = loader;
    }

    private static String toClassName(final String classFile) {
        return classFile.replace('/', '.');
    }

    /**
     * 获取类文件操作对象，操作字节码文件
     * 
     * @return
     * @throws IOException
     */
    public CtClass getCtClass() throws IOException {
        if (ctClass != null)
            return ctClass;

        final ClassPool classPool = new ClassPool(true);
        if (loader == null) {
            classPool.appendClassPath(new LoaderClassPath(ClassLoader.getSystemClassLoader()));
        } else {
            classPool.appendClassPath(new LoaderClassPath(loader));
        }
        // CtClass对象被writeFile(),toClass()或者toBytecode()转换成了类对象，Javassist将会冻结此CtClass对象.
        // jvm不允许类的重复加载。
        final CtClass clazz = classPool.makeClass(new ByteArrayInputStream(classFileBuffer), false);
        // 解冻CtClass对象，解冻后又可以随意修改类
        clazz.defrost();

        this.ctClass = clazz;
        return clazz;
    }

    public URL getLocationUrl() throws IOException {
        return getLocationUrlOfClass(getCtClass());
    }

    public String getClassName() {
        return className;
    }

    private boolean modified = false;

    public boolean isModified() {
        return modified;
    }

    public void setModified() {
        this.modified = true;
    }

    public ClassLoader getClassLoader() {
        return loader;
    }

}
