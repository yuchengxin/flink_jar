package com.catyee.test.common.assient;

import com.catyee.test.common.utils.FlinkSchemaUtils;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.util.StringUtils;

import java.util.LinkedHashMap;
import java.util.Map;

public class TableSchemaBuilder {
    private final Map<String, String> columns = new LinkedHashMap<>();
    private final Map<String, String> params = new LinkedHashMap<>();

    private TableSchemaBuilder() {
    }

    public static TableSchemaBuilder builder() {
        return new TableSchemaBuilder();
    }

    public TableSchemaBuilder addColumn(String colName, String colType) {
        if (!StringUtils.isNullOrWhitespaceOnly(colName) && !StringUtils.isNullOrWhitespaceOnly(colType)) {
            columns.put(colName, colType);
            return this;
        }
        String errMsg = String.format("col name/type cannot be null, col name:%s, col type:%s", colName, colType);
        throw new RuntimeException(errMsg);
    }

    /**
     * 添加表设置，比如：'format.type' = 'raw-tracker'
     * @param paramKey
     * @param paramValue
     * @return
     */
    public TableSchemaBuilder addParams(String paramKey, String paramValue) {
        if (!StringUtils.isNullOrWhitespaceOnly(paramKey) && !StringUtils.isNullOrWhitespaceOnly(paramValue)) {
            columns.put(paramKey, paramValue);
            return this;
        }
        String errMsg = String.format("param key/value cannot be null, param key:%s, param value:%s", paramKey, paramValue);
        throw new RuntimeException(errMsg);
    }

    public TableSchema build() {
        int i = 0;
        for (Map.Entry<String, String> entry : columns.entrySet()) {
            String nameKey = String.format("schema.%d.name", i);
            params.put(nameKey, entry.getKey());
            String valueKey = String.format("schema.%d.data-type", i);
            params.put(valueKey, entry.getValue());
            i++;
        }
        return FlinkSchemaUtils.deriveSchema(params);
    }
}
