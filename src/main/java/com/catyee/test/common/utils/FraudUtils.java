package com.catyee.test.common.utils;


import com.catyee.test.common.entity.ParTableEntry;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

public class FraudUtils {

    public static ParTableEntry fraudParTableEntry(long id, String prefix) {
        LocalDateTime createTime = fraudDate(null);
        return fraudParTableEntry(id, prefix, createTime);
    }

    /**
     * create_time == update_time
     * @param id
     * @param prefix
     * @param createTime
     * @return
     */
    public static ParTableEntry fraudParTableEntry(long id, String prefix, LocalDateTime createTime) {
        return ParTableEntry.builder()
                .id(id)
                .username(prefix + "_" + id)
                .password(fraudRandom(1, 10) > 8 ? null : prefix + "_" + id)
                .gender(fraudRandom(0, 1) == 0 ? "M" : "W")
                .married(fraudRandom(0, 1) == 0 ? "Y" : "N")
                .createTime(createTime)
                .updateTime(createTime)
                .version(fraudRandom(1, 10))
                .build();
    }

    public static <T> List<T> fraudList(Supplier<T> supplier) {
        List<T> list = new ArrayList<>();
        int end = fraudRandom(1, 10);
        for (int i = 0; i < end; i++) {
            list.add(supplier.get());
        }
        return list;
    }

    public static List<String> fraudStringList(String str) {
        List<String> list = new ArrayList<>();
        int end = fraudRandom(1, 10);
        for (int i = 0; i < end; i++) {
            list.add(fraudUnique(str));
        }
        return list;
    }

    public static Map<String, String> fraudMap(String factor) {
        Map<String, String> result = new HashMap<>();
        int end = fraudRandom(1, 10);
        for (int i = 0; i < end; i++) {
            result.put(fraudUnique(factor + "_key"), fraudUnique(factor + "_value"));
        }
        return result;
    }

    public static Map<String, String> fraudMap(String factor, int start, int end) {
        Map<String, String> result = new HashMap<>();
        for (int i = start; i <= end; i++) {
            result.put(factor + "_key_" + i, fraudUnique(factor + "_value"));
        }
        return result;
    }

    public static LocalDateTime fraudPositiveDate(LocalDateTime date) {
        date = date == null ? LocalDateTime.now() : date;
        int intervalYear = fraudRandom(0, 5);
        int intervalMonth = fraudRandom(0, 12);
        int intervalDay = fraudRandom(0, 30);
        int intervalHour = fraudRandom(0, 24);
        int intervalMinutes = fraudRandom(0, 60);
        return date.plusYears(intervalYear).plusMonths(intervalMonth).plusDays(intervalDay).plusHours(intervalHour).plusMinutes(intervalMinutes);
    }

    public static LocalDateTime fraudPositiveDateWithHour(LocalDateTime date) {
        date = date == null ? LocalDateTime.now() : date;
        int intervalHour = fraudRandom(1, 24);
        int intervalMinutes = fraudRandom(1, 60);
        return date.plusHours(intervalHour).plusMinutes(intervalMinutes);
    }

    public static LocalDateTime fraudDate(LocalDateTime date) {
        date = date == null ? LocalDateTime.now() : date;
        int intervalYear = fraudRandom(-5, 5);
        int intervalMonth = fraudRandom(-12, 12);
        int intervalDay = fraudRandom(-30, 30);
        int intervalHour = fraudRandom(-24, 24);
        int intervalMinutes = fraudRandom(-60, 60);
        return date.plusYears(intervalYear).plusMonths(intervalMonth).plusDays(intervalDay).plusHours(intervalHour).plusMinutes(intervalMinutes);
    }

    public static String fraudUnique(String str) {
        return str + "_" + UUID.randomUUID().toString().replace("-", "");
    }

    /**
     * generate random int:[min, max)
     * @param min
     * @param max
     * @return
     */
    public static Integer fraudRandom(int min, int max) {
        return ThreadLocalRandom.current().nextInt(min, max);
    }

    /**
     * generate random long:[min, max)
     * @param min
     * @param max
     * @return
     */
    public static Long fraudRandom(long min, long max) {
        return ThreadLocalRandom.current().nextLong(min, max);
    }

    public static String fraudString(int min, int max) {
        if (min <= 0) {
            throw new RuntimeException("min cannot less than 0");
        }
        int len = fraudRandom(min, max);
        return fraudString(len);
    }

    public static String fraudString(int len) {
        if (len == 0) {
            return "";
        }
        char[] arr = new char[len];
        Arrays.fill(arr, 'a');
        return new String(arr);
    }
}
