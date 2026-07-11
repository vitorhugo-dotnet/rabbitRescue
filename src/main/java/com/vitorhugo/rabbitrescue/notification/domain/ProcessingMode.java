package com.vitorhugo.rabbitrescue.notification.domain;

import java.util.Locale;

public enum ProcessingMode {
    SUCCESS,
    FAIL,
    FLAKY,
    INVALID;

    public static ProcessingMode fromPath(String value) {
        try {
            return valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (RuntimeException exception) {
            throw new IllegalArgumentException(
                    "Modo inválido. Use success, fail, flaky ou invalid.",
                    exception
            );
        }
    }
}
