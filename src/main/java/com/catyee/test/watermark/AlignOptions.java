package com.catyee.test.watermark;

import lombok.Getter;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.io.Serializable;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.configuration.ConfigOptions.key;

@Getter
public class AlignOptions implements Serializable {

    public static final ConfigOption<String> FAST_KAFKA_BOOTSTRAP_SERVERS =
            key("fastBootstrapServers")
                    .stringType()
                    .defaultValue("")
                    .withDescription("");

    public static final ConfigOption<String> FAST_CLUSTER_NAME =
            key("fastClusterName")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("");

    public static final ConfigOption<String> FAST_TOPIC =
            key("fastTopic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("");

    public static final ConfigOption<String> FAST_GROUP_ID =
            key("fastGroupId")
                    .stringType()
                    .defaultValue("fast_group_id")
                    .withDescription("");

    public static final ConfigOption<String> SLOW_BOOTSTRAP_SERVERS =
            key("slowBootstrapServers")
                    .stringType()
                    .defaultValue("")
                    .withDescription("");

    public static final ConfigOption<String> SLOW_CLUSTER_NAME =
            key("slowClusterName")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("");

    public static final ConfigOption<String> SLOW_TOPIC =
            key("slowTopic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("");

    public static final ConfigOption<String> SLOW_GROUP_ID =
            key("slowGroupId")
                    .stringType()
                    .defaultValue("slow_group_id")
                    .withDescription("");

    public static final ConfigOption<Boolean> DRY_RUN =
            key("dryRun")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("");

    public static final ConfigOption<Duration> MAX_DRIFT =
            key("maxDrift")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(5))
                    .withDescription("");

    public static final ConfigOption<Duration> UPDATE_INTERVAL =
            key("updateInterval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription("");

    public static final ConfigOption<OffsetResetStrategy> OFFSET_REST_STRATEGY =
            key("offsetRestStrategy")
                    .enumType(OffsetResetStrategy.class)
                    .defaultValue(OffsetResetStrategy.LATEST)
                    .withDescription("");

    public static final ConfigOption<Long> START_TIMESTAMP =
            key("startTimestamp")
                    .longType()
                    .noDefaultValue()
                    .withDescription("");

    public static final ConfigOption<Duration> WATERMARK_LAG =
            key("watermarkLag")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDescription("watermark落后一段时间");

    public static final ConfigOption<Boolean> ALIGN_ENABLED =
            key("alignEnabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("");

    public static final ConfigOption<Duration> WINDOW_LENGTH =
            key("windowLength")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(1))
                    .withDescription("");

    public static final ConfigOption<Boolean> EMIT_WATERMARK_PERIOD =
            key("emitWatermarkPeriod")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("");

    private static final Set<ConfigOption<?>> requiredKeys;
    private static final Set<ConfigOption<?>> supportedKeys;

    static {
        Set<ConfigOption<?>> required = new HashSet<>();
        required.add(FAST_CLUSTER_NAME);
        required.add(FAST_TOPIC);
        required.add(SLOW_CLUSTER_NAME);
        required.add(SLOW_TOPIC);
        required.add(MAX_DRIFT);
        requiredKeys = Collections.unmodifiableSet(required);

        Set<ConfigOption<?>> supported = new HashSet<>();
        supported.add(FAST_KAFKA_BOOTSTRAP_SERVERS);
        supported.add(SLOW_BOOTSTRAP_SERVERS);
        supported.add(DRY_RUN);
        supported.add(UPDATE_INTERVAL);
        supported.add(FAST_GROUP_ID);
        supported.add(SLOW_GROUP_ID);
        supported.add(OFFSET_REST_STRATEGY);
        supported.add(START_TIMESTAMP);
        supported.add(WATERMARK_LAG);
        supported.add(ALIGN_ENABLED);
        supported.add(WINDOW_LENGTH);
        supported.add(EMIT_WATERMARK_PERIOD);
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

    private final String fastBootstrapServers;
    private final String fastClusterName;
    private final String fastTopic;
    private final String fastGroupId;
    private final String slowBootstrapServers;
    private final String slowClusterName;
    private final String slowTopic;
    private final String slowGroupId;
    private final boolean dryRun;
    private final Duration maxDrift;
    private final Duration updateInterval;
    private final OffsetResetStrategy offsetResetStrategy;
    private final Long startTimestamp;
    private final Duration watermarkLag;
    private final boolean alignEnabled;
    private final Duration windowLength;
    private final boolean emitWatermarkPeriod;

    private AlignOptions(Configuration conf) {
        this.fastBootstrapServers = conf.get(FAST_KAFKA_BOOTSTRAP_SERVERS);
        this.fastClusterName = conf.get(FAST_CLUSTER_NAME);
        this.fastTopic = conf.get(FAST_TOPIC);
        this.fastGroupId = conf.get(FAST_GROUP_ID);
        this.slowBootstrapServers = conf.get(SLOW_BOOTSTRAP_SERVERS);
        this.slowClusterName = conf.get(SLOW_CLUSTER_NAME);
        this.slowTopic = conf.get(SLOW_TOPIC);
        this.slowGroupId = conf.get(SLOW_GROUP_ID);
        this.dryRun = conf.get(DRY_RUN);
        this.maxDrift = conf.get(MAX_DRIFT);
        this.updateInterval = conf.get(UPDATE_INTERVAL);
        this.offsetResetStrategy = conf.get(OFFSET_REST_STRATEGY);
        this.startTimestamp = conf.getOptional(START_TIMESTAMP).orElse(null);
        this.watermarkLag = conf.get(WATERMARK_LAG);
        this.alignEnabled = conf.get(ALIGN_ENABLED);
        this.windowLength = conf.get(WINDOW_LENGTH);
        this.emitWatermarkPeriod = conf.get(EMIT_WATERMARK_PERIOD);
    }

    public static AlignOptions of(Configuration conf) {
        return new AlignOptions(conf);
    }
}
