package com.vitorhugo.rabbitrescue.notification.api;

import com.vitorhugo.rabbitrescue.notification.domain.NotificationStatus;
import java.time.Instant;
import java.util.UUID;

public record CreateNotificationResponse(
        UUID messageId,
        String correlationId,
        NotificationStatus status,
        Instant acceptedAt
) {
}
