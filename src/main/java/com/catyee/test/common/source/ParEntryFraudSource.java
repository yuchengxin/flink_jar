package com.catyee.test.common.source;

import com.catyee.test.common.entity.ParTableEntry;
import com.catyee.test.common.utils.FraudUtils;
import com.catyee.test.common.utils.TimeUtils;
import com.catyee.test.common.utils.UniqueIdGenerator;
import lombok.Builder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.LocalDateTime;

/**
 * Artificial source for sensor measurements (temperature and wind speed) of a pre-defined set of
 * sensors (per parallel instance) creating measurements for two locations (inside the bounding
 * boxes of Germany (DE) and the USA (US)) in SI units (°C and km/h).
 */
@SuppressWarnings("WeakerAccess")
public class ParEntryFraudSource extends RichParallelSourceFunction<ParTableEntry> {

    private volatile boolean running = true;

    private final boolean isOrder;
    private final String prefix;
    private final long rowsPerSecond;
    @Nullable
    private final Long numberOfRows;
    private final Duration recordMinInterval;
    private final Duration recordMaxInterval;

    private transient int outputSoFar;
    private transient int toOutput;

    @Builder
    public ParEntryFraudSource(boolean isOrder, String prefix, long rowsPerSecond, @Nullable Long numberOfRows,
                               Duration recordMinInterval, Duration recordMaxInterval) {
        this.isOrder = isOrder;
        this.prefix = prefix;
        this.rowsPerSecond = rowsPerSecond;
        this.numberOfRows = numberOfRows;
        this.recordMinInterval = recordMinInterval;
        this.recordMaxInterval = recordMaxInterval;
    }

    @Override
    public void open(final Configuration parameters) {
        if (numberOfRows != null) {
            final int stepSize = getRuntimeContext().getNumberOfParallelSubtasks();
            final int taskIdx = getRuntimeContext().getIndexOfThisSubtask();

            final int baseSize = (int) (numberOfRows / stepSize);
            toOutput = (numberOfRows % stepSize > taskIdx) ? baseSize + 1 : baseSize;
        }
    }

    @Override
    public void run(SourceContext<ParTableEntry> ctx) {
        double taskRowsPerSecond = (double) rowsPerSecond / getRuntimeContext().getNumberOfParallelSubtasks();
        long nextReadTime = System.currentTimeMillis();
        LocalDateTime baseTime = LocalDateTime.now().minusDays(1);

        while (running) {
            for (int i = 0; i < taskRowsPerSecond; i++) {
                if (running && (numberOfRows == null || outputSoFar < toOutput)) {
                    synchronized (ctx.getCheckpointLock()) {
                        outputSoFar++;
                        long plusMilliseconds = FraudUtils.fraudRandom(recordMinInterval.toMillis(), recordMaxInterval.toMillis());
                        LocalDateTime localDateTime = TimeUtils.plusMilliseconds(baseTime, plusMilliseconds);
                        baseTime = localDateTime;
                        if (!isOrder && FraudUtils.fraudRandom(1, 10) <= 1) { // 如果设置乱序，有1/10的概率生成乱序数据
                            localDateTime = baseTime.minusMinutes(FraudUtils.fraudRandom(-60, -1));
                        }
                        ParTableEntry entry = FraudUtils.fraudParTableEntry(UniqueIdGenerator.genId(), prefix, localDateTime);
                        ctx.collect(entry);
                    }
                } else {
                    return;
                }
            }

            nextReadTime += 1000;
            long toWaitMs = nextReadTime - System.currentTimeMillis();
            while (toWaitMs > 0) {
                try {
                    Thread.sleep(toWaitMs);
                } catch (Exception e) {
                    Thread.currentThread().interrupt();
                }
                toWaitMs = nextReadTime - System.currentTimeMillis();
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
