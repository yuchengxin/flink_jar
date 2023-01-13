package com.catyee.test.common.entity;

import com.catyee.test.common.assient.ResolvedSchemaBuilder;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;

import java.time.LocalDateTime;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Data
public class ParTableEntry {
    private long id;
    private String username;
    private String password;
    private String gender;
    private String married;
    @JsonProperty("create_time")
    private LocalDateTime createTime;
    @JsonProperty("update_time")
    private LocalDateTime updateTime;
    private int version;

    @Getter
    private static final ResolvedSchema defaultParTableSchema;

    static {
        defaultParTableSchema = buildSchema(false);
    }

    public static ResolvedSchema buildSchema(boolean addPrimaryKey) {
        ResolvedSchemaBuilder builder = ResolvedSchemaBuilder.builder()
                .addColumn("id", new AtomicDataType(new IntType()).notNull())
                .addColumn("username", new AtomicDataType(new VarCharType()).nullable())
                .addColumn("password", new AtomicDataType(new VarCharType()).nullable())
                .addColumn("gender", new AtomicDataType(new VarCharType()).nullable())
                .addColumn("married", new AtomicDataType(new VarCharType()).nullable())
                .addColumn("create_time", new AtomicDataType(new TimestampType(3)).notNull())
                .addColumn("update_time", new AtomicDataType(new TimestampType(3)).notNull())
                .addColumn("version", new AtomicDataType(new IntType()).notNull());
        if (addPrimaryKey) {
            builder.setPrimaryKeys("id");
        }
        return builder.build();
    }
}
