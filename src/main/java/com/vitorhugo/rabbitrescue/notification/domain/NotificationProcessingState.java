package com.vitorhugo.rabbitrescue.notification.domain;

import java.time.Instant;
import java.util.UUID;

public record NotificationProcessingState(
        UUID messageId,
        String correlationId,
        NotificationStatus status,
        int attempt,
        String lastError,
        Instant updatedAt
) {
}
