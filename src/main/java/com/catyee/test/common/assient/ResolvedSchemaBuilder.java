package com.catyee.test.common.assient;

import com.catyee.test.common.utils.CheckUtils;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ResolvedSchemaBuilder {

    private ResolvedSchemaBuilder() {
    }

    private final List<Column> columns = new ArrayList<>();
    private final List<String> primaryKeys = new ArrayList<>();

    public static ResolvedSchemaBuilder builder() {
        return new ResolvedSchemaBuilder();
    }

    public ResolvedSchemaBuilder addColumn(@Nonnull String name, @Nonnull DataType dataType) {
        Column column = Column.physical(name, dataType);
        columns.add(column);
        return this;
    }

    public ResolvedSchemaBuilder setPrimaryKeys(String... pks) {
        primaryKeys.addAll(Arrays.asList(pks));
        return this;
    }

    public ResolvedSchema build() {
        UniqueConstraint uniqueConstraint = null;
        if (!primaryKeys.isEmpty()) {
            CheckUtils.check(columns.stream().map(Column::getName).collect(Collectors.toList()).containsAll(primaryKeys),
                    "columns must contain all primary keys.");
            uniqueConstraint = UniqueConstraint.primaryKey("primary_key", primaryKeys);
        }
        return new ResolvedSchema(columns, Collections.emptyList(), uniqueConstraint);
    }
}
