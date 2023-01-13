package com.catyee.test.common.utils;

import com.catyee.test.common.exception.CatyeeErrCode;
import com.catyee.test.common.exception.CatyeeException;
import com.catyee.test.common.exception.CatyeeRuntimeException;
import com.catyee.test.common.function.ThrowableConsumer;
import com.catyee.test.common.function.ThrowableFunction;
import com.catyee.test.common.function.ThrowableRunnable;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.concurrent.Callable;

/**
 * 用于执行某个方法，同时将异常进行转换
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class FunctionUtils {

    public static void run(ThrowableRunnable runnable) {
        run(runnable, null);
    }

    public static void run(ThrowableRunnable runnable, String msgWhenError) {
        try {
            runnable.run();
        } catch (CatyeeException me) {
            throw me.toRuntimeException();
        } catch (CatyeeRuntimeException me) {
            throw me;
        } catch (Exception e) {
            throw new CatyeeRuntimeException(CatyeeErrCode.UNEXPECTED_ERROR, msgWhenError, e);
        }
    }

    public static <V> V call(Callable<V> callable) {
        return call(callable, null);
    }

    public static <V> V call(Callable<V> callable, String msgWhenError) {
        try {
            return callable.call();
        } catch (CatyeeException me) {
            throw me.toRuntimeException();
        } catch (CatyeeRuntimeException me) {
            throw me;
        } catch (Exception e) {
            throw new CatyeeRuntimeException(CatyeeErrCode.UNEXPECTED_ERROR, msgWhenError, e);
        }
    }

    public static <T> void accept(T t, ThrowableConsumer<T> consumer) {
        accept(t, consumer, null);
    }

    public static <T> void accept(T t, ThrowableConsumer<T> consumer, String msgWhenError) {
        try {
            consumer.accept(t);
        } catch (CatyeeException me) {
            throw me.toRuntimeException();
        } catch (CatyeeRuntimeException me) {
            throw me;
        } catch (Exception e) {
            if (t != null) {
                msgWhenError = msgWhenError == null ? "Arg t is : " + t : msgWhenError + " Arg t is : " + t;
            }
            throw new CatyeeRuntimeException(CatyeeErrCode.UNEXPECTED_ERROR, msgWhenError, e);
        }
    }

    public static <T, R> R apply(T t, ThrowableFunction<T, R> function) {
        return apply(t, function, null);
    }

    public static <T, R> R apply(T t, ThrowableFunction<T, R> function, String msgWhenError) {
        try {
            return function.apply(t);
        } catch (CatyeeException me) {
            throw me.toRuntimeException();
        } catch (CatyeeRuntimeException me) {
            throw me;
        } catch (Exception e) {
            if (t != null) {
                msgWhenError = msgWhenError == null ? "Arg t is : " + t : msgWhenError + " Arg t is : " + t;
            }
            throw new CatyeeRuntimeException(CatyeeErrCode.UNEXPECTED_ERROR, msgWhenError, e);
        }
    }
}
