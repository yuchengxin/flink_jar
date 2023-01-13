package com.catyee.test.common.utils;

import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.types.DataType;

import java.util.Map;

public class FlinkSchemaUtils {

    private static final String SCHEMA = "schema";
    private static final String SCHEMA_PROCTIME = "proctime";
    private static final String SCHEMA_FROM = "from";
    private static final String ROWTIME_TIMESTAMPS_TYPE = "rowtime.timestamps.type";
    private static final String ROWTIME_TIMESTAMPS_TYPE_VALUE_FROM_FIELD = "from-field";
    private static final String ROWTIME_TIMESTAMPS_FROM = "rowtime.timestamps.from";

    public static TableSchema deriveSchema(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties();
        descriptorProperties.putProperties(properties);

        final TableSchema.Builder builder = TableSchema.builder();

        final TableSchema tableSchema = descriptorProperties.getTableSchema(SCHEMA);
        for (int i = 0; i < tableSchema.getFieldCount(); i++) {
            final TableColumn tableColumn = tableSchema.getTableColumns().get(i);
            final String fieldName = tableColumn.getName();
            final DataType dataType = tableColumn.getType();
            if (!tableColumn.isPhysical()) {
                // skip non-physical columns
                continue;
            }
            final boolean isProctime =
                    descriptorProperties
                            .getOptionalBoolean(SCHEMA + '.' + i + '.' + SCHEMA_PROCTIME)
                            .orElse(false);
            final String timestampKey = SCHEMA + '.' + i + '.' + ROWTIME_TIMESTAMPS_TYPE;
            final boolean isRowtime = descriptorProperties.containsKey(timestampKey);
            if (!isProctime && !isRowtime) {
                // check for aliasing
                final String aliasName =
                        descriptorProperties
                                .getOptionalString(SCHEMA + '.' + i + '.' + SCHEMA_FROM)
                                .orElse(fieldName);
                builder.field(aliasName, dataType);
            }
            // only use the rowtime attribute if it references a field
            else if (isRowtime
                    && descriptorProperties.isValue(
                    timestampKey, ROWTIME_TIMESTAMPS_TYPE_VALUE_FROM_FIELD)) {
                final String aliasName =
                        descriptorProperties.getString(
                                SCHEMA + '.' + i + '.' + ROWTIME_TIMESTAMPS_FROM);
                builder.field(aliasName, dataType);
            }
        }

        return builder.build();
    }
}
