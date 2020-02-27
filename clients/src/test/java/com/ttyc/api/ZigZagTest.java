package com.ttyc.api;

import org.junit.Test;

public class ZigZagTest {

    @Test
    public void testEncode() {
        System.out.println(zigZaglize(-2));

        System.out.println(dezigZag(3));
    }

    int zigZaglize(int n){
        return (n <<1) ^ (n >>31);
    }

    int dezigZag(int n) {
        return (n >>> 1) ^ -(n & 1);
    }
}

