package com.catyee.test.common.exception;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by yuankui on 19-7-31.
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class CatyeeException extends Exception {
    private final ErrorCode<CatyeeErrCode> errorCode;

    public CatyeeException(ErrorCode<CatyeeErrCode> errorCode) {
        super(buildExceptionMessage(errorCode, null));
        this.errorCode = errorCode;
    }

    public CatyeeException(ErrorCode<CatyeeErrCode> errorCode, String errorMsg) {
        super(buildExceptionMessage(errorCode, errorMsg));
        this.errorCode = errorCode;
    }

    public CatyeeException(ErrorCode<CatyeeErrCode> errorCode, Throwable e) {
        super(buildExceptionMessage(errorCode, null), e);
        this.errorCode = errorCode;
    }

    public CatyeeException(ErrorCode<CatyeeErrCode> errorCode, String errorMsg, Throwable e) {
        super(buildExceptionMessage(errorCode, errorMsg), e);
        this.errorCode = errorCode;
    }

    public CatyeeException(ErrorCode<CatyeeErrCode> errorCode, String format, Object... args) {
        super(buildExceptionMessage(errorCode, String.format(format, args)));
        this.errorCode = errorCode;
    }

    public CatyeeRuntimeException toRuntimeException() {
        return new CatyeeRuntimeException(errorCode, getMessage(), getCause());
    }

    private static String buildExceptionMessage(ErrorCode<CatyeeErrCode> errorCode, String errorMsg) {
        return StringUtils.isBlank(errorMsg) ?
                "[" + errorCode.getErrorCodeName() + " 原因:" + errorCode.getErrorCodeDesc() + "]" :
                "[" + errorCode.getErrorCodeName() + " 原因:" + errorCode.getErrorCodeDesc() + "] 具体信息:" + errorMsg;
    }

}
