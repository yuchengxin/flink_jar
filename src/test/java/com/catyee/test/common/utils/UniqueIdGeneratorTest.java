package com.catyee.test.common.utils;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.*;

public class UniqueIdGeneratorTest {

    private transient ExecutorService executorService;

    @Before
    public void before() {
        this.executorService = Executors.newFixedThreadPool(5);
    }

    @Test
    public void genId() throws Exception {
        for (int  j = 0; j < 5; j++) {
            executorService.submit(() -> {
                for (int i = 0; i < 1000; i++) {
                    System.out.println(UniqueIdGenerator.genId());
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
        }
        Thread.currentThread().join();
        executorService.shutdownNow();
    }
}