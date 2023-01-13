package com.catyee.test.common.serializer;

import com.catyee.test.common.entity.ParTableEntry;
import com.catyee.test.common.utils.JsonUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ParTableJsonDeserializationSchema implements KafkaRecordDeserializationSchema<ParTableEntry> {
    private static final Logger LOG = LoggerFactory.getLogger(ParTableJsonDeserializationSchema.class);

    @Override
    public TypeInformation<ParTableEntry> getProducedType() {
        return TypeInformation.of(ParTableEntry.class);
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        KafkaRecordDeserializationSchema.super.open(context);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<ParTableEntry> collector) throws IOException {
        byte[] values = consumerRecord.value();
        ParTableEntry parTableEntry = JsonUtils.getObject(values, new TypeReference<ParTableEntry>() {});
        if (parTableEntry == null) {
            if (LOG.isInfoEnabled()) {
                LOG.info("deserialize bytes from kafka to ParTableEntry is null. bytes: {}", values == null ? null : new String(values));
            }
            return;
        }
        collector.collect(parTableEntry);
    }
}
