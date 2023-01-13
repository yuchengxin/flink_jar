package com.catyee.test.common.assient;

import org.apache.flink.util.StringUtils;

import java.util.Properties;

public class KafkaPropertiesBuilder {

    private final Properties properties = new Properties();

    private KafkaPropertiesBuilder() {}

    public static KafkaPropertiesBuilder builder() {
        return new KafkaPropertiesBuilder();
    }

    public KafkaPropertiesBuilder addParams(String paramKey, String paramValue) {
        if (!StringUtils.isNullOrWhitespaceOnly(paramKey) && !StringUtils.isNullOrWhitespaceOnly(paramValue)) {
            properties.put(paramKey, paramValue);
            return this;
        }
        String errMsg = String.format("param key/value cannot be null, param key:%s, param value:%s", paramKey, paramValue);
        throw new RuntimeException(errMsg);
    }

    public Properties build() {
        return this.properties;
    }
}
