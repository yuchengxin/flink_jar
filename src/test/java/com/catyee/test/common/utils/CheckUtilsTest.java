package com.catyee.test.common.utils;

import org.junit.Test;

import static org.junit.Assert.*;

public class CheckUtilsTest {

    @Test
    public void check() {
        CheckUtils.check(true, "test");
        CheckUtils.check(true, "test %s:%d", "a", 1);
    }
}