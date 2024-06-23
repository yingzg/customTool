package com.gzy.custom.ttlagent.agent;

import java.io.IOException;

import javassist.CannotCompileException;
import javassist.NotFoundException;

public interface TtlTransformlet {

    void doTransform(ClassInfo classInfo) throws IOException, NotFoundException, CannotCompileException;
}
