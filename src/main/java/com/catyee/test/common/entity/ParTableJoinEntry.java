package com.catyee.test.common.entity;

import com.catyee.test.common.assient.ResolvedSchemaBuilder;
import lombok.*;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;

import java.time.LocalDateTime;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Data
public class ParTableJoinEntry {
    private long id;
    private String username1;
    private String username2;
    private String password1;
    private String password2;
    private String gender1;
    private String gender2;
    private String married1;
    private String married2;
    private LocalDateTime updateTime1;
    private LocalDateTime updateTime2;

    @Getter
    private static final ResolvedSchema defaultSchema;
    @Getter
    private static final ResolvedSchema defaultSchemaWithPrimaryKey;

    static {
        defaultSchema = buildSchema(false);
        defaultSchemaWithPrimaryKey = buildSchema(true);
    }

    public static ResolvedSchema buildSchema(boolean addPrimaryKey) {
        ResolvedSchemaBuilder builder = ResolvedSchemaBuilder.builder()
                .addColumn("id", new AtomicDataType(new IntType()).notNull())
                .addColumn("username1", new AtomicDataType(new VarCharType()).nullable())
                .addColumn("username2", new AtomicDataType(new VarCharType()).nullable())
                .addColumn("password1", new AtomicDataType(new VarCharType()).nullable())
                .addColumn("password2", new AtomicDataType(new VarCharType()).nullable())
                .addColumn("gender1", new AtomicDataType(new VarCharType()).nullable())
                .addColumn("gender2", new AtomicDataType(new VarCharType()).nullable())
                .addColumn("married1", new AtomicDataType(new VarCharType()).nullable())
                .addColumn("married2", new AtomicDataType(new VarCharType()).nullable())
                .addColumn("update_time1", new AtomicDataType(new TimestampType(3)).notNull())
                .addColumn("update_time2", new AtomicDataType(new TimestampType(3)).notNull());
        if (addPrimaryKey) {
            builder.setPrimaryKeys("id");
        }
        return builder.build();
    }

    public RowData toRowData() {
        GenericRowData rowData =  new GenericRowData(11);
        rowData.setField(0, id);
        rowData.setField(1, username1);
        rowData.setField(2, username2);
        rowData.setField(3, password1);
        rowData.setField(4, password2);
        rowData.setField(5, gender1);
        rowData.setField(6, gender2);
        rowData.setField(7, married1);
        rowData.setField(8, married2);
        rowData.setField(9, updateTime1);
        rowData.setField(10, updateTime2);
        return rowData;
    }
}
