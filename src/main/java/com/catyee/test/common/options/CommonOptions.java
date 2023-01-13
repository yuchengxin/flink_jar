package com.catyee.test.common.options;

import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

public class CommonOptions {
    public static final ConfigOption<Boolean> DISABLE_CHAINING =
            key("disableChaining")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("");
}
