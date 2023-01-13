package com.catyee.test.common.serializer;

import com.catyee.test.common.entity.ParTableEntry;
import com.catyee.test.common.utils.JsonUtils;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class ParTableJsonSerializationSchema implements KafkaSerializationSchema<ParTableEntry> {

    private final String topic;

    public ParTableJsonSerializationSchema(String topic) {
        this.topic = topic;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {
        KafkaSerializationSchema.super.open(context);
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(ParTableEntry parTableEntry, @Nullable Long timestamp) {
        byte[] keyBytes = String.valueOf(parTableEntry.getId()).getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = JsonUtils.getJsonBytes(parTableEntry);
        long time = timestamp == null ? System.currentTimeMillis() : timestamp;
        return new ProducerRecord<>(topic, null, time, keyBytes, valueBytes);
    }
}
