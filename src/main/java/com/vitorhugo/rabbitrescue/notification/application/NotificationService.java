package com.vitorhugo.rabbitrescue.notification.application;

import com.vitorhugo.rabbitrescue.notification.api.CreateNotificationRequest;
import com.vitorhugo.rabbitrescue.notification.api.CreateNotificationResponse;
import com.vitorhugo.rabbitrescue.notification.domain.NotificationRequested;
import com.vitorhugo.rabbitrescue.notification.domain.NotificationStatus;
import com.vitorhugo.rabbitrescue.notification.messaging.NotificationMessagePublisher;
import java.time.Clock;
import java.time.Instant;
import java.util.UUID;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class NotificationService {

    private final NotificationMessagePublisher publisher;
    private final NotificationStatusStore statusStore;
    private final Clock clock = Clock.systemUTC();

    public CreateNotificationResponse requestNotification(
            CreateNotificationRequest request,
            String requestedCorrelationId
    ) {
        UUID messageId = UUID.randomUUID();
        String correlationId = normalizeCorrelationId(requestedCorrelationId);
        Instant acceptedAt = Instant.now(clock);

        NotificationRequested event = new NotificationRequested(
                messageId,
                request.recipients(),
                request.subject(),
                request.content(),
                request.processingMode(),
                acceptedAt
        );

        publisher.publishNew(event, correlationId);
        statusStore.markPublished(messageId, correlationId);

        return new CreateNotificationResponse(
                messageId,
                correlationId,
                NotificationStatus.PUBLISHED,
                acceptedAt
        );
    }

    private String normalizeCorrelationId(String requestedCorrelationId) {
        if (requestedCorrelationId == null || requestedCorrelationId.isBlank()) {
            return UUID.randomUUID().toString();
        }

        return requestedCorrelationId.trim();
    }
}
