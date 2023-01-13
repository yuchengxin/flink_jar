package com.catyee.test.common.exception;

/**
 * ErrCode只用于指明大的错误类别，不提供错误的具体信息，具体信息需要在抛出异常的时候额外提供
 *
 * Created by yuankui on 19-7-31.
 */
public enum CatyeeErrCode implements ErrorCode<CatyeeErrCode> {
    // generic error
    UNEXPECTED_ERROR(10000, "GENERIC-10000", "不可预知的错误"),
    OPERATE_ERROR(10001, "GENERIC-10001", "非法操作"),
    CONFIG_ERROR(10002, "GENERIC-10002", "配置项错误"),
    CONFIG_FILE_ERROR(10003, "GENERIC-10003", "配置文件错误"),
    NOT_FOUND_ERROR(10004, "GENERIC-10004", "对象不存在"),
    UNSUPPORTED_ERROR(10005, "GENERIC-10005", "功能暂不支持"),
    COMPILE_ERROR(10006, "GENERIC-10006", "编译错误"),
    JSON_ERROR(10007, "GENERIC-10007", "JSON错误"),
    INIT_ERROR(10008, "GENERIC-10008","初始化错误"),
    PARSE_ERROR(10009, "GENERIC-10009","解析错误"),
    FILESYSTEM_ERROR(10010, "GENERIC-10010", "文件系统错误"),
    GUARDIAN_ERROR(10011, "GENERIC-10011", "Guardian错误"),
    FILE_OP_ERROR(10012, "GENERIC-10012", "文件操作错误"),
    UNSUPPORTED_METHOD(10013, "GENERIC-10013", "调用不支持的方法"),

    // JDBC 错误
    JDBC_CONNECTION_ERROR(40000, "JDBC-40000", "JDBC连接异常, 请检查具体配置及权限"),
    JDBC_SQL_ERROR(40001, "JDBC-40001", "执行SQL失败,请检查异常的报错信息"),
    JDBC_META_ERROR(40002, "JDBC-40002", "获取JDBC元信息失败,请检查异常的报错信息"),
    JDBC_QUERY_TIMEOUT(40003, "JDBC-40003", "数据库执行sql超时"),
    JDBC_COL_TYPE_ERROR(40004, "JDBC-40004", "列类型错误"),
    JDBC_DRIVER_ERROR(40005, "JDBC-40005", "驱动类错误"),
    JDBC_DDL_ERROR(40006, "JDBC-40006", "DDL执行异常"),

    // 迁移任务错误
    SYNC_PRODUCE_ERROR(50004, "JOB-50004", "instance produce出错"),
    SYNC_CONSUME_ERROR(50005, "JOB-50005", "adapter consume出错"),
    META_PARSE_ERROR(50006, "JOB-50006", "元数据分析出错"),

    // 转换错误
    META_TRANS_ERROR(60001, "TRANS-60001", "元数据转换出错"),
    ;

    private final int value;
    private final String name;
    private final String desc;

    CatyeeErrCode(int value, String name, String desc) {
        this.value = value;
        this.name = name;
        this.desc = desc;
    }

    @Override
    public int getErrorCodeValue() {
        return value;
    }

    @Override
    public String getErrorCodeName() {
        return name;
    }

    @Override
    public String getErrorCodeDesc() {
        return desc;
    }
}
