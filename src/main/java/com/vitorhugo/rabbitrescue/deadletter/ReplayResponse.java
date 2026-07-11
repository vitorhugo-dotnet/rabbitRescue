package com.vitorhugo.rabbitrescue.deadletter;

import java.util.UUID;

public record ReplayResponse(
        UUID messageId,
        String correlationId,
        String status
) {
}
