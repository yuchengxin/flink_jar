package com.catyee.test.watermark;

import com.catyee.test.common.assient.KafkaPropertiesBuilder;
import com.catyee.test.common.entity.ParTableEntry;
import com.catyee.test.common.entity.ParTableJoinEntry;
import com.catyee.test.common.options.CommonOptions;
import com.catyee.test.common.serializer.ParTableJsonDeserializationSchema;
import com.catyee.test.common.sink.ConsoleDynamicSinkFunction;
import com.catyee.test.common.sink.options.ConsoleSinkOptions;
import com.catyee.test.common.utils.TimeUtils;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumState;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.data.RowData;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collections;
import java.util.Properties;
import java.util.stream.Collectors;

public class WatermarkAlignmentTask {
    private static final Logger LOG = LoggerFactory.getLogger(WatermarkAlignmentTask.class);
    private static final String DEFAULT_WATERMARK_GROUP = "align_task_group";

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        Configuration config = parameters.getConfiguration();

        if (LOG.isInfoEnabled()) {
            LOG.info("WatermarkAlignmentTask config options:\n{}\n", config.toMap().entrySet().stream()
                    .map(entry -> entry.getKey() + ": " + entry.getValue())
                    .collect(Collectors.joining(";\n")));
        }
        AlignOptions.checkOptions(config);
        AlignOptions alignOptions = AlignOptions.of(config);

        boolean disableChaining = config.get(CommonOptions.DISABLE_CHAINING);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if (disableChaining) {
            env.disableOperatorChaining();
        }

        OffsetsInitializer offsetsInitializer = getOffsetsInitializer(alignOptions);

        Properties fastProperties = KafkaPropertiesBuilder.builder()
                .addParams("kafka.cluster.name", alignOptions.getFastClusterName())
                .build();
        Source<ParTableEntry, KafkaPartitionSplit, KafkaSourceEnumState> fastSource = KafkaSource
                .<ParTableEntry>builder()
                .setBootstrapServers(alignOptions.getFastBootstrapServers())
                .setProperties(fastProperties)
                .setGroupId(alignOptions.getFastGroupId())
                .setTopics(Collections.singletonList(alignOptions.getFastTopic()))
                .setDeserializer(new ParTableJsonDeserializationSchema())
                .setStartingOffsets(offsetsInitializer)
                .build();

        Properties slowProperties = KafkaPropertiesBuilder.builder()
                .addParams("kafka.cluster.name", alignOptions.getSlowClusterName())
                .build();
        Source<ParTableEntry, KafkaPartitionSplit, KafkaSourceEnumState> slowSource = KafkaSource
                .<ParTableEntry>builder()
                .setBootstrapServers(alignOptions.getSlowBootstrapServers())
                .setProperties(slowProperties)
                .setGroupId(alignOptions.getSlowGroupId())
                .setTopics(Collections.singletonList(alignOptions.getSlowTopic()))
                .setDeserializer(new ParTableJsonDeserializationSchema())
                .setStartingOffsets(offsetsInitializer)
                .build();

        ConsoleSinkOptions sinkOptions = new ConsoleSinkOptions(alignOptions.isDryRun());
        ConsoleDynamicSinkFunction sinkFunction = new ConsoleDynamicSinkFunction(sinkOptions, ParTableJoinEntry.getDefaultSchema());

        DataStream<ParTableEntry> fast = env.fromSource(fastSource, getWatermarkStrategy(alignOptions), "fastSource");
        DataStream<ParTableEntry> slow = env.fromSource(slowSource, getWatermarkStrategy(alignOptions), "slowSource");

        fast.join(slow)
                .where((KeySelector<ParTableEntry, Long>) ParTableEntry::getId)
                .equalTo((KeySelector<ParTableEntry, Long>) ParTableEntry::getId)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(alignOptions.getWindowLength().toMillis())))
                .apply((JoinFunction<ParTableEntry, ParTableEntry, RowData>) (entry1, entry2) -> ParTableJoinEntry.builder()
                        .id(entry1.getId())
                        .username1(entry1.getUsername())
                        .username2(entry2.getUsername())
                        .password1(entry1.getPassword())
                        .password2(entry2.getPassword())
                        .gender1(entry1.getGender())
                        .gender2(entry2.getGender())
                        .married1(entry1.getMarried())
                        .married2(entry2.getMarried())
                        .updateTime1(entry1.getUpdateTime())
                        .updateTime2(entry2.getUpdateTime())
                        .build()
                        .toRowData())
                .addSink(sinkFunction).name("console");

        env.execute("watermark_align_test_task");
    }

    private static OffsetsInitializer getOffsetsInitializer(AlignOptions alignOptions) {
        Long startTimestamp = alignOptions.getStartTimestamp();
        if (startTimestamp != null) {
            return OffsetsInitializer.timestamp(startTimestamp);
        }
        OffsetResetStrategy strategy = alignOptions.getOffsetResetStrategy();
        if (strategy == OffsetResetStrategy.EARLIEST) {
            return OffsetsInitializer.earliest();
        }
        return OffsetsInitializer.latest();
    }

    private static WatermarkStrategy<ParTableEntry> getWatermarkStrategy(AlignOptions alignOptions) {
        AlignWatermarkGenerator generator = new AlignWatermarkGenerator(alignOptions);
        WatermarkStrategy<ParTableEntry> watermarkStrategy = WatermarkStrategy.forGenerator(context -> generator)
                .withTimestampAssigner((TimestampAssignerSupplier<ParTableEntry>) context
                        -> (event, timestamp) -> TimeUtils.localDateTimeToLong(event.getCreateTime()));
        if (alignOptions.isAlignEnabled()) {
            watermarkStrategy = watermarkStrategy.withWatermarkAlignment(DEFAULT_WATERMARK_GROUP,
                    alignOptions.getMaxDrift(), alignOptions.getUpdateInterval());
        }
        return watermarkStrategy;
    }

    private static class AlignWatermarkGenerator implements Serializable, WatermarkGenerator<ParTableEntry> {

        private final AlignOptions alignOptions;
        private transient long currentWatermark;

        public AlignWatermarkGenerator(AlignOptions alignOptions) {
            this.alignOptions = alignOptions;
        }

        @Override
        public void onEvent(ParTableEntry parTableEntry, long eventTime, WatermarkOutput watermarkOutput) {
            currentWatermark = TimeUtils.localDateTimeToLong(parTableEntry.getCreateTime()) - alignOptions.getWatermarkLag().toMillis();
            if (!alignOptions.isEmitWatermarkPeriod()) {
                watermarkOutput.emitWatermark(new Watermark(currentWatermark));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
            if (alignOptions.isEmitWatermarkPeriod()) {
                watermarkOutput.emitWatermark(new Watermark(currentWatermark));
            }
        }
    }
}
