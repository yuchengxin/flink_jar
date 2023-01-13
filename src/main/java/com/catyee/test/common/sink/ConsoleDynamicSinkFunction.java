package com.catyee.test.common.sink;

import com.catyee.test.common.sink.options.ConsoleSinkOptions;
import com.catyee.test.common.utils.RowDataUtils;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/** ConsoleDynamicSinkFunction. */
public class ConsoleDynamicSinkFunction extends RichSinkFunction<RowData> {
    private static final Logger LOG = LoggerFactory.getLogger(ConsoleDynamicSinkFunction.class);

    private final ConsoleSinkOptions options;
    private final ColumnPart fields;
    private long seq = 0;

    // only used in upsert mode
    private boolean isUpsert;
    private ColumnPart pkColumns;
    private ColumnPart otherColumns;

    public ConsoleDynamicSinkFunction(ConsoleSinkOptions options, ResolvedSchema tableSchema) {
        this.options = options;
        List<String> fieldNames = tableSchema.getColumnNames();
        List<DataType> dataTypes = tableSchema.getColumnDataTypes();
        List<Integer> positions = new ArrayList<>();
        for (int i = 0; i < fieldNames.size(); i++) {
            positions.add(i);
        }
        this.fields = new ColumnPart(fieldNames, dataTypes, positions);
        Optional<UniqueConstraint> uniqueConstraint = tableSchema.getPrimaryKey();
        if (uniqueConstraint.isPresent()) {
            List<String> pkNames = uniqueConstraint.get().getColumns();
            List<DataType> pkDataTypes = new ArrayList<>(pkNames.size());
            List<Integer> pkPos = new ArrayList<>(pkNames.size());
            List<String> otherColNames = new ArrayList<>();
            List<DataType> otherColTypes = new ArrayList<>();
            List<Integer> otherColPos = new ArrayList<>();

            for (int i = 0; i < fieldNames.size(); i++) {
                String name = fieldNames.get(i);
                DataType type = dataTypes.get(i);
                if (pkNames.contains(name)) {
                    pkDataTypes.add(type);
                    pkPos.add(i);
                } else {
                    otherColNames.add(name);
                    otherColTypes.add(type);
                    otherColPos.add(i);
                }
            }
            this.pkColumns = new ColumnPart(pkNames, pkDataTypes, pkPos);
            this.otherColumns = new ColumnPart(otherColNames, otherColTypes, otherColPos);
            this.isUpsert = true;
        }
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {
        if (options.isDryRun()) {
            return;
        }
        if (isUpsert) {
            upsert(value);
        } else {
            insertOnly(value);
        }
    }

    private void insertOnly(RowData value) {
        String msg = getMsg(value, fields);
        seq++;
        LOG.info("Append mode, Seq: {}, RowData: {}", seq, msg);
    }

    private void upsert(RowData rowData) {
        String keyMsg = getMsg(rowData, pkColumns);
        String valueMsg = getMsg(rowData, otherColumns);
        seq++;
        LOG.info(
                "Upsert mode, Seq: {}, kind: {}, [primary key]: {} [other fields]:{}",
                seq,
                rowData.getRowKind(),
                keyMsg,
                valueMsg);
    }

    private String getMsg(RowData value, ColumnPart columnPart) {
        List<String> fieldNames = columnPart.names;
        List<DataType> dataTypes = columnPart.dataTypes;
        List<Integer> positions = columnPart.positions;

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fieldNames.size(); i++) {
            String fieldName = fieldNames.get(i);
            sb.append(fieldName).append(": ");

            DataType dataType = dataTypes.get(i);
            int pos = positions.get(i);
            Object val = RowDataUtils.getData(value, pos, dataType.getLogicalType());
            if (val == null) {
                sb.append("null");
            } else {
                String str = null;
                if (val instanceof byte[]) {
                    str = Base64.getEncoder().encodeToString((byte[]) val);
                } else if (val instanceof Object[]) {
                    str = Arrays.toString((Object[]) val);
                } else if (val instanceof List) {
                    str =
                            ((List<?>) val)
                                    .stream()
                                            .map(v -> v == null ? "null" : v.toString())
                                            .collect(Collectors.joining(", "));
                } else {
                    str = val.toString();
                }
                sb.append(str);
            }
            sb.append("; ");
        }
        return sb.toString();
    }

    private static class ColumnPart implements Serializable {
        private final List<String> names;
        private final List<DataType> dataTypes;
        private final List<Integer> positions;

        public ColumnPart(List<String> names, List<DataType> dataTypes, List<Integer> positions) {
            this.names = names;
            this.dataTypes = dataTypes;
            this.positions = positions;
        }
    }
}
