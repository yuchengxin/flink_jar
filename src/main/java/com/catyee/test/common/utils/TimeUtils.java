package com.catyee.test.common.utils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.TimeZone;

public class TimeUtils {

    public static long localDateTimeToLong(LocalDateTime localDateTime) {
        ZonedDateTime zdt = ZonedDateTime.of(localDateTime, ZoneId.systemDefault());
        return zdt.toInstant().toEpochMilli();
    }

    public static LocalDateTime longToLocalDateTime(long timestamp) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), TimeZone
                        .getDefault().toZoneId());
    }

    public static LocalDateTime plusMilliseconds(LocalDateTime localDateTime, long milliseconds) {
        long time = localDateTimeToLong(localDateTime);
        return longToLocalDateTime(time + milliseconds);
    }
}
