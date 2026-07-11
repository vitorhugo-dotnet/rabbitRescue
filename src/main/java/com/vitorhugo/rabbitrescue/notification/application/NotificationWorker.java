package com.vitorhugo.rabbitrescue.notification.application;

import static com.vitorhugo.rabbitrescue.notification.messaging.RabbitNames.HEADER_ATTEMPT;
import static com.vitorhugo.rabbitrescue.notification.messaging.RabbitNames.NOTIFICATIONS_QUEUE;

import com.vitorhugo.rabbitrescue.config.MessagingProperties;
import com.vitorhugo.rabbitrescue.notification.domain.NotificationRequested;
import com.vitorhugo.rabbitrescue.notification.domain.NotificationStatus;
import com.vitorhugo.rabbitrescue.notification.messaging.NotificationMessagePublisher;
import com.vitorhugo.rabbitrescue.provider.FakeNotificationProvider;
import com.vitorhugo.rabbitrescue.provider.PermanentProviderException;
import com.vitorhugo.rabbitrescue.provider.TransientProviderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
public class NotificationWorker {

    private static final Logger LOGGER = LoggerFactory.getLogger(NotificationWorker.class);

    private final FakeNotificationProvider provider;
    private final NotificationMessagePublisher publisher;
    private final NotificationStatusStore statusStore;
    private final MessagingProperties properties;

    public NotificationWorker(
            FakeNotificationProvider provider,
            NotificationMessagePublisher publisher,
            NotificationStatusStore statusStore,
            MessagingProperties properties
    ) {
        this.provider = provider;
        this.publisher = publisher;
        this.statusStore = statusStore;
        this.properties = properties;
    }

    @RabbitListener(queues = NOTIFICATIONS_QUEUE)
    public void consume(NotificationRequested event, Message message) {
        String correlationId = requireCorrelationId(message);
        int attempt = readAttempt(message);

        try (MDC.MDCCloseable ignoredCorrelation = MDC.putCloseable("correlationId", correlationId);
             MDC.MDCCloseable ignoredAttempt = MDC.putCloseable("attempt", Integer.toString(attempt))) {

            statusStore.update(
                    event.messageId(),
                    correlationId,
                    NotificationStatus.PROCESSING,
                    attempt,
                    null
            );

            provider.send(event, attempt);

            statusStore.update(
                    event.messageId(),
                    correlationId,
                    NotificationStatus.PROCESSED,
                    attempt,
                    null
            );

            LOGGER.info("notification_processed messageId={} destination=processed", event.messageId());
        } catch (TransientProviderException exception) {
            handleTransientFailure(event, correlationId, attempt, exception);
        } catch (PermanentProviderException exception) {
            sendToDeadLetter(event, correlationId, attempt, exception);
        }
    }

    private void handleTransientFailure(
            NotificationRequested event,
            String correlationId,
            int attempt,
            TransientProviderException exception
    ) {
        if (attempt >= properties.maxAttempts()) {
            sendToDeadLetter(event, correlationId, attempt, exception);
            return;
        }

        int nextAttempt = attempt + 1;
        publisher.publishRetry(event, correlationId, nextAttempt, exception);
        statusStore.update(
                event.messageId(),
                correlationId,
                NotificationStatus.RETRY_SCHEDULED,
                nextAttempt,
                exception.getMessage()
        );

        LOGGER.warn(
                "notification_retry_scheduled messageId={} nextAttempt={} destination={}",
                event.messageId(),
                nextAttempt,
                "notifications.retry.queue"
        );
    }

    private void sendToDeadLetter(
            NotificationRequested event,
            String correlationId,
            int attempt,
            RuntimeException exception
    ) {
        publisher.publishDeadLetter(event, correlationId, attempt, exception);
        statusStore.update(
                event.messageId(),
                correlationId,
                NotificationStatus.DEAD_LETTERED,
                attempt,
                exception.getMessage()
        );

        LOGGER.error(
                "notification_dead_lettered messageId={} destination=notifications.dlq reason={}",
                event.messageId(),
                exception.getMessage()
        );
    }

    private String requireCorrelationId(Message message) {
        String correlationId = message.getMessageProperties().getCorrelationId();
        if (correlationId == null || correlationId.isBlank()) {
            throw new IllegalStateException("Mensagem sem correlationId");
        }
        return correlationId;
    }

    private int readAttempt(Message message) {
        Object value = message.getMessageProperties().getHeaders().get(HEADER_ATTEMPT);
        if (value instanceof Number number) {
            return number.intValue();
        }
        throw new IllegalStateException("Mensagem sem header x-attempt");
    }
}
