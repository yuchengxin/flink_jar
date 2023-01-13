package com.catyee.test.common.utils;

import javax.annotation.Nonnull;
import java.util.function.BooleanSupplier;

public class CheckUtils {

    public static void check(BooleanSupplier supplier, @Nonnull String msg) {
        check(supplier.getAsBoolean(), msg);
    }

    public static void check(BooleanSupplier supplier, @Nonnull String format, Object... args) {
        check(supplier.getAsBoolean(), format, args);
    }

    public static void check(boolean addition, @Nonnull String msg) {
        check(addition, msg, (Object) null);
    }

    public static void check(boolean addition, @Nonnull String format, Object... args) {
        String msg = String.format(format, args);
        if (!addition) {
            throw new RuntimeException(msg);
        }
    }
}
