package com.gzy.custom.ttlthreadlocal;

import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class TestMain {

    @Test
    public void testDemo1() {

        int value=512;
        int v2=  (32 - Integer.numberOfLeadingZeros(value - 1));
        int v1= 1 << (32 - Integer.numberOfLeadingZeros(value - 1));
        System.out.println("v1 = " + v1);
        System.out.println("v2 = " + v2);
       long v3= Long.MAX_VALUE/512/(10^6);
        System.out.println("v3 = " + v3);
    }
}
