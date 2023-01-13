package com.catyee.test.common.exception;

/**
 * Created by yuankui on 19-7-31.
 */
public interface ErrorCode<T extends Enum<T>> {
    int getErrorCodeValue();
    String getErrorCodeName();
    String getErrorCodeDesc();

    default String ofString() {
        return "[" + getErrorCodeName() + "]:" + getErrorCodeDesc();
    }
}
