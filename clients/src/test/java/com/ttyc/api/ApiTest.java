package com.ttyc.api;

import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class ApiTest {

    @Test
    public void testDisJoin() {
        Set<Integer> set1 = new HashSet<>();
        set1.add(1);
        set1.add(2);
        set1.add(3);

        Set<Integer> set2 = new HashSet<>();
        set2.add(4);
        set2.add(5);
        set2.add(6);

        // 有无共同元素
        boolean disjoint = Collections.disjoint(set1, set2);
        System.out.println("disjoint = " + disjoint);

    }

    @Test
    public void testCacl() {
        long elapsedTime = 3;
        long metadataStart = 1000;
        long metadataEnd = 1005;

        elapsedTime += (metadataEnd - metadataStart);

        elapsedTime += metadataEnd - metadataStart;

        System.out.println("elapsedTime = " + elapsedTime);
    }

}
