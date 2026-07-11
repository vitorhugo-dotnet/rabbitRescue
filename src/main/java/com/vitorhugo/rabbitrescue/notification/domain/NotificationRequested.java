package com.vitorhugo.rabbitrescue.notification.domain;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Evento imutável transportado pelo RabbitMQ.
 *
 * processingMode é opcional. Quando informado, a mensagem controla o cenário
 * do fake provider e os testes ficam independentes do estado global.
 */
public record NotificationRequested(
        UUID messageId,
        List<String> recipients,
        String subject,
        String content,
        ProcessingMode processingMode,
        Instant requestedAt
) {
    public NotificationRequested {
        recipients = List.copyOf(recipients);
    }
}
