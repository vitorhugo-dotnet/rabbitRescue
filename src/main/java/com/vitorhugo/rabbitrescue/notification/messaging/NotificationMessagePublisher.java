package com.vitorhugo.rabbitrescue.notification.messaging;

import static com.vitorhugo.rabbitrescue.notification.messaging.RabbitNames.HEADER_ATTEMPT;
import static com.vitorhugo.rabbitrescue.notification.messaging.RabbitNames.HEADER_ERROR_MESSAGE;
import static com.vitorhugo.rabbitrescue.notification.messaging.RabbitNames.HEADER_ERROR_TYPE;
import static com.vitorhugo.rabbitrescue.notification.messaging.RabbitNames.HEADER_REPLAYED;
import static com.vitorhugo.rabbitrescue.notification.messaging.RabbitNames.NOTIFICATIONS_DLQ_ROUTING_KEY;
import static com.vitorhugo.rabbitrescue.notification.messaging.RabbitNames.NOTIFICATIONS_DLX;
import static com.vitorhugo.rabbitrescue.notification.messaging.RabbitNames.NOTIFICATIONS_EXCHANGE;
import static com.vitorhugo.rabbitrescue.notification.messaging.RabbitNames.NOTIFICATIONS_RETRY_ROUTING_KEY;
import static com.vitorhugo.rabbitrescue.notification.messaging.RabbitNames.NOTIFICATIONS_ROUTING_KEY;

import com.vitorhugo.rabbitrescue.notification.domain.NotificationRequested;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

/**
 * Único ponto responsável por montar propriedades e headers AMQP.
 */
@Component
public class NotificationMessagePublisher {

    private final RabbitTemplate rabbitTemplate;

    public NotificationMessagePublisher(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    public void publishNew(NotificationRequested event, String correlationId) {
        send(NOTIFICATIONS_EXCHANGE, NOTIFICATIONS_ROUTING_KEY, event, correlationId, 1, null, false);
    }

    public void publishRetry(
            NotificationRequested event,
            String correlationId,
            int nextAttempt,
            Throwable error
    ) {
        send(
                NOTIFICATIONS_DLX,
                NOTIFICATIONS_RETRY_ROUTING_KEY,
                event,
                correlationId,
                nextAttempt,
                error,
                false
        );
    }

    public void publishDeadLetter(
            NotificationRequested event,
            String correlationId,
            int attempt,
            Throwable error
    ) {
        send(
                NOTIFICATIONS_DLX,
                NOTIFICATIONS_DLQ_ROUTING_KEY,
                event,
                correlationId,
                attempt,
                error,
                false
        );
    }

    public void replay(NotificationRequested event, String correlationId) {
        send(NOTIFICATIONS_EXCHANGE, NOTIFICATIONS_ROUTING_KEY, event, correlationId, 1, null, true);
    }

    private void send(
            String exchange,
            String routingKey,
            NotificationRequested event,
            String correlationId,
            int attempt,
            Throwable error,
            boolean replayed
    ) {
        rabbitTemplate.convertAndSend(exchange, routingKey, event, message -> {
            var properties = message.getMessageProperties();
            properties.setMessageId(event.messageId().toString());
            properties.setCorrelationId(correlationId);
            properties.setHeader(HEADER_ATTEMPT, attempt);

            if (error != null) {
                properties.setHeader(HEADER_ERROR_TYPE, error.getClass().getSimpleName());
                properties.setHeader(HEADER_ERROR_MESSAGE, safeMessage(error));
            }

            if (replayed) {
                properties.setHeader(HEADER_REPLAYED, true);
            }

            return message;
        });
    }

    private String safeMessage(Throwable error) {
        String message = error.getMessage();
        return message == null || message.isBlank()
                ? error.getClass().getSimpleName()
                : message;
    }
}
