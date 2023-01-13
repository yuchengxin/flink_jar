package com.catyee.test.common.utils;

import org.junit.Test;

import java.sql.Time;
import java.time.LocalDateTime;

import static org.junit.Assert.*;

public class TimeUtilsTest {

    @Test
    public void localDateTimeToLong() {
        TimeUtils.localDateTimeToLong(LocalDateTime.now());
        System.out.println();
    }

    @Test
    public void longToLocalDateTime() {
    }
}