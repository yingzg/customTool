package com.gzy.custom.db2es;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class Db2esApplicationTests {

    @Test
    void contextLoads() {}

    public static void main(String[] args) {
        System.out.println("toHex(16) = " + toHex(16));
        System.out.println("toHex(16) = " + toHex(-16));
    }

    public static String toHex(int num) {
        if (num == 0) return "0";
        StringBuilder sb = new StringBuilder();
        while (num != 0) {
            int u = num & 15;
            char c = (char)(u + '0');
            if (u >= 10) c = (char)(u - 10 + 'a');
            sb.append(c);
            num >>>= 4;
        }
        return sb.reverse().toString();
    }

}
