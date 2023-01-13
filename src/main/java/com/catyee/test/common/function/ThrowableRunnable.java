package com.catyee.test.common.function;

@FunctionalInterface
public interface ThrowableRunnable {
    void run() throws Exception;
}