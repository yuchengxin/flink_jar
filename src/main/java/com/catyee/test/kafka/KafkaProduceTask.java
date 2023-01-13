package com.catyee.test.kafka;

import com.catyee.test.common.options.CommonOptions;
import com.catyee.test.common.entity.ParTableEntry;
import com.catyee.test.common.serializer.ParTableJsonSerializationSchema;
import com.catyee.test.common.source.ParEntryFraudSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;


public class KafkaProduceTask {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaProduceTask.class);

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        Configuration config = parameters.getConfiguration();

        if (LOG.isInfoEnabled()) {
            LOG.info("kafka-produce-task config options:\n{}\n", config.toMap().entrySet().stream()
                    .map(entry -> entry.getKey() + ": " + entry.getValue())
                    .collect(Collectors.joining(";\n")));
        }
        KafkaTaskOptions.checkOptions(config);

        String topic = config.get(KafkaTaskOptions.TOPIC);
        String prefix = config.get(KafkaTaskOptions.PREFIX);
        boolean isOrder = config.get(KafkaTaskOptions.IS_ORDER);
        Optional<Long> numberOfRowsOptional = config.getOptional(KafkaTaskOptions.NUMBER_OF_ROWS);
        Long numberOfRows = numberOfRowsOptional.orElse(null);
        long rowsPerSecond = config.get(KafkaTaskOptions.ROWS_PER_SECOND);
        boolean disableChaining = config.get(CommonOptions.DISABLE_CHAINING);
        Duration recordMaxInterval = config.get(KafkaTaskOptions.RECORD_MAX_INTERVAL);
        Duration recordMinInterval = config.get(KafkaTaskOptions.RECORD_MIN_INTERVAL);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        if (disableChaining) {
            env.disableOperatorChaining();
        }

        ParEntryFraudSource source = ParEntryFraudSource.builder()
                .isOrder(isOrder)
                .prefix(prefix)
                .recordMinInterval(recordMinInterval)
                .recordMaxInterval(recordMaxInterval)
                .numberOfRows(numberOfRows)
                .rowsPerSecond(rowsPerSecond)
                .build();

        DataStreamSource<ParTableEntry> stream = env.addSource(source);

        Properties kafkaProperties = genKafkaProperties(config);
        FlinkKafkaProducer<ParTableEntry> myProducer = new FlinkKafkaProducer<>(
                topic,
                new ParTableJsonSerializationSchema(topic),
                kafkaProperties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        stream.addSink(myProducer);

        env.execute("kafka-produce-task");
    }

    private static Properties genKafkaProperties(Configuration config) {
        Optional<String> bootstrapOptions = config.getOptional(KafkaTaskOptions.BOOTSTRAP_SERVERS);
        String clusterName = config.get(KafkaTaskOptions.CLUSTER_NAME);
        Properties properties = new Properties();
        bootstrapOptions.ifPresent(bootstraps -> properties.setProperty("bootstrap.servers", bootstraps));
        properties.setProperty("kafka.cluster.name", clusterName);
        return properties;
    }
}
