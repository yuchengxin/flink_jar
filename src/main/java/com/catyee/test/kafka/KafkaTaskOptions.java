package com.catyee.test.kafka;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.configuration.ConfigOptions.key;

public class KafkaTaskOptions {
    public static final ConfigOption<String> BOOTSTRAP_SERVERS =
            key("bootstrapServers")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("");

    public static final ConfigOption<String> CLUSTER_NAME =
            key("clusterName")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("");

    public static final ConfigOption<String> TOPIC =
            key("topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("");

    public static final ConfigOption<Long> NUMBER_OF_ROWS =
            key("numberOfRows")
                    .longType()
                    .noDefaultValue()
                    .withDescription("");

    public static final ConfigOption<Long> ROWS_PER_SECOND =
            key("rowsPerSecond")
                    .longType()
                    .noDefaultValue()
                    .withDescription("");

    public static final ConfigOption<String> PREFIX =
            key("prefix")
                    .stringType()
                    .defaultValue("PAR")
                    .withDescription("");

    public static final ConfigOption<Boolean> IS_ORDER =
            key("isOrder")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("");

    public static final ConfigOption<Duration> RECORD_MIN_INTERVAL =
            key("recordMinInterval")
                    .durationType()
                    .defaultValue(Duration.ofMillis(1))
                    .withDescription("");

    public static final ConfigOption<Duration> RECORD_MAX_INTERVAL =
            key("recordMaxInterval")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("");

    private static final Set<ConfigOption<?>> requiredKeys;
    private static final Set<ConfigOption<?>> supportedKeys;

    static {
        Set<ConfigOption<?>> required = new HashSet<>();
        required.add(CLUSTER_NAME);
        required.add(TOPIC);
        required.add(PREFIX);
        required.add(ROWS_PER_SECOND);
        required.add(RECORD_MAX_INTERVAL);
        requiredKeys = Collections.unmodifiableSet(required);

        Set<ConfigOption<?>> supported = new HashSet<>();
        supported.add(BOOTSTRAP_SERVERS);
        supported.add(NUMBER_OF_ROWS);
        supported.add(IS_ORDER);
        supported.add(RECORD_MIN_INTERVAL);
        supportedKeys = Collections.unmodifiableSet(supported);
    }

    public static Set<ConfigOption<?>> getRequiredOptions() {
        return requiredKeys;
    }

    public static Set<ConfigOption<?>> getSupportedOptions() {
        return supportedKeys;
    }


    public static void checkOptions(Configuration config) {
        boolean all = getRequiredOptions().stream().allMatch(config::contains);
        if (!all) {
            throw new RuntimeException("Incomplete required option.");
        }
    }

}
