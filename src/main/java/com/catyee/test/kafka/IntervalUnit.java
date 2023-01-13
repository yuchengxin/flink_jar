package com.catyee.test.kafka;

import java.util.Arrays;

public enum IntervalUnit {
    SECOND("s"),
    MILLISECOND("ms"),
    ;

    private final String alias;

    IntervalUnit(String alias) {
        this.alias = alias;
    }

    public static IntervalUnit lookup(String alias) {
        return Arrays.stream(values())
                .filter(u -> u.alias.equalsIgnoreCase(alias))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("cannot parse interval unit, alias must be 's' or 'ms', current is: " + alias));
    }

    public boolean isSecond() {
        return this == SECOND;
    }

    public boolean isMilliSecond() {
        return this == MILLISECOND;
    }
}
