package com.catyee.test.common.sink.options;

import lombok.Getter;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;

import java.io.Serializable;

/** ConsoleSinkOptions. */
@Getter
public class ConsoleSinkOptions implements Serializable {
    public static final ConfigOption<Boolean> DRY_RUN =
            ConfigOptions.key("connector.dry-run")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "switch to print the log, `true` will not, `false` will print");

    private final boolean dryRun;

    public ConsoleSinkOptions(ReadableConfig config) {
        this.dryRun = config.get(DRY_RUN);
    }

    public ConsoleSinkOptions(boolean dryRun) {
        this.dryRun = dryRun;
    }
}
