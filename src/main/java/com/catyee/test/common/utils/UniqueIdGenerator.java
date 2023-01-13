package com.catyee.test.common.utils;

import java.util.concurrent.atomic.AtomicLong;

public class UniqueIdGenerator {
    private static final AtomicLong idHolder = new AtomicLong(0);

    public static long genId() {
        return idHolder.incrementAndGet();
    }
}
