package com.vitorhugo.rabbitrescue.deadletter;

import com.vitorhugo.rabbitrescue.notification.domain.NotificationRequested;
import java.util.UUID;

public record DeadLetterResponse(
        UUID messageId,
        String correlationId,
        int attempt,
        String errorType,
        String errorMessage,
        NotificationRequested payload
) {
}
