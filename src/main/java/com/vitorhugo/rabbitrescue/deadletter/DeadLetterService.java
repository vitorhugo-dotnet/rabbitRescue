package com.vitorhugo.rabbitrescue.deadletter;

import static com.vitorhugo.rabbitrescue.notification.messaging.RabbitNames.HEADER_ATTEMPT;
import static com.vitorhugo.rabbitrescue.notification.messaging.RabbitNames.HEADER_ERROR_MESSAGE;
import static com.vitorhugo.rabbitrescue.notification.messaging.RabbitNames.HEADER_ERROR_TYPE;
import static com.vitorhugo.rabbitrescue.notification.messaging.RabbitNames.NOTIFICATIONS_DLQ;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;
import com.vitorhugo.rabbitrescue.config.MessagingProperties;
import com.vitorhugo.rabbitrescue.notification.application.NotificationStatusStore;
import com.vitorhugo.rabbitrescue.notification.domain.NotificationRequested;
import com.vitorhugo.rabbitrescue.notification.messaging.NotificationMessagePublisher;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.stereotype.Service;

/**
 * Operações administrativas da DLQ.
 *
 * AMQP 0-9-1 não possui um comando de browse. Para listar sem destruir,
 * usamos basic.get sem auto-ack e devolvemos todas as entregas ao final.
 */
@Service
public class DeadLetterService {

    private final ConnectionFactory connectionFactory;
    private final ObjectMapper objectMapper;
    private final NotificationMessagePublisher publisher;
    private final NotificationStatusStore statusStore;
    private final MessagingProperties properties;
    private final ReentrantLock operationLock = new ReentrantLock();

    public DeadLetterService(
            ConnectionFactory connectionFactory,
            ObjectMapper objectMapper,
            NotificationMessagePublisher publisher,
            NotificationStatusStore statusStore,
            MessagingProperties properties
    ) {
        this.connectionFactory = connectionFactory;
        this.objectMapper = objectMapper;
        this.publisher = publisher;
        this.statusStore = statusStore;
        this.properties = properties;
    }

    public List<DeadLetterResponse> list(int requestedLimit) {
        int limit = Math.min(requestedLimit, properties.deadLetterBrowseLimit());
        operationLock.lock();

        Connection connection = null;
        Channel channel = null;
        List<Long> deliveryTags = new ArrayList<>();

        try {
            connection = connectionFactory.createConnection();
            channel = connection.createChannel(false);

            List<DeadLetterResponse> messages = new ArrayList<>();
            for (int index = 0; index < limit; index++) {
                GetResponse response = channel.basicGet(NOTIFICATIONS_DLQ, false);
                if (response == null) {
                    break;
                }

                deliveryTags.add(response.getEnvelope().getDeliveryTag());
                messages.add(toResponse(response));
            }

            return List.copyOf(messages);
        } catch (IOException exception) {
            throw new AmqpException("Não foi possível consultar a DLQ", exception);
        } finally {
            requeue(channel, deliveryTags);
            close(channel, connection);
            operationLock.unlock();
        }
    }

    public ReplayResponse replay(UUID messageId) {
        operationLock.lock();

        Connection connection = null;
        Channel channel = null;
        List<Long> untouchedTags = new ArrayList<>();
        GetResponse selected = null;

        try {
            connection = connectionFactory.createConnection();
            channel = connection.createChannel(false);

            for (int index = 0; index < properties.deadLetterBrowseLimit(); index++) {
                GetResponse response = channel.basicGet(NOTIFICATIONS_DLQ, false);
                if (response == null) {
                    break;
                }

                NotificationRequested event = readEvent(response);
                if (event.messageId().equals(messageId)) {
                    selected = response;
                    String correlationId = requireCorrelationId(response);

                    /*
                     * Publica primeiro e remove da DLQ somente depois que a
                     * republicação foi aceita localmente pelo RabbitTemplate.
                     */
                    publisher.replay(event, correlationId);
                    channel.basicAck(response.getEnvelope().getDeliveryTag(), false);

                    /*
                     * O replay é diferente da publicação inicial: aqui podemos
                     * substituir explicitamente DEAD_LETTERED por PUBLISHED.
                     */
                    statusStore.markReplayPublished(
                            event.messageId(),
                            correlationId
                    );

                    requeue(channel, untouchedTags);
                    untouchedTags.clear();

                    return new ReplayResponse(
                            event.messageId(),
                            correlationId,
                            "REPLAYED"
                    );
                }

                untouchedTags.add(response.getEnvelope().getDeliveryTag());
            }

            throw new DeadLetterNotFoundException(messageId);
        } catch (IOException exception) {
            if (selected != null) {
                untouchedTags.add(selected.getEnvelope().getDeliveryTag());
            }
            throw new AmqpException(
                    "Não foi possível reenfileirar a mensagem",
                    exception
            );
        } catch (RuntimeException exception) {
            if (selected != null) {
                untouchedTags.add(selected.getEnvelope().getDeliveryTag());
            }
            throw exception;
        } finally {
            requeue(channel, untouchedTags);
            close(channel, connection);
            operationLock.unlock();
        }
    }

    private DeadLetterResponse toResponse(GetResponse response) {
        NotificationRequested event = readEvent(response);
        Map<String, Object> headers = response.getProps().getHeaders();

        return new DeadLetterResponse(
                event.messageId(),
                requireCorrelationId(response),
                numberHeader(headers, HEADER_ATTEMPT),
                stringHeader(headers, HEADER_ERROR_TYPE),
                stringHeader(headers, HEADER_ERROR_MESSAGE),
                event
        );
    }

    private NotificationRequested readEvent(GetResponse response) {
        byte[] body = response.getBody();

        if (body == null || body.length == 0) {
            throw new IllegalStateException(
                    "Mensagem da DLQ sem payload. messageId="
                            + response.getProps().getMessageId()
            );
        }

        try {
            return objectMapper.readValue(body, NotificationRequested.class);
        } catch (IOException exception) {
            throw new IllegalStateException(
                    "Payload inválido encontrado na DLQ. messageId="
                            + response.getProps().getMessageId(),
                    exception
            );
        }
    }

    private String requireCorrelationId(GetResponse response) {
        String correlationId = response.getProps().getCorrelationId();
        if (correlationId == null || correlationId.isBlank()) {
            throw new IllegalStateException(
                    "Mensagem da DLQ sem correlationId"
            );
        }
        return correlationId;
    }

    private int numberHeader(Map<String, Object> headers, String name) {
        Object value = headers == null ? null : headers.get(name);
        if (value instanceof Number number) {
            return number.intValue();
        }
        return 0;
    }

    private String stringHeader(Map<String, Object> headers, String name) {
        Object value = headers == null ? null : headers.get(name);
        return value == null ? null : value.toString();
    }

    private void requeue(Channel channel, List<Long> deliveryTags) {
        if (channel == null || !channel.isOpen()) {
            return;
        }

        for (Long deliveryTag : deliveryTags) {
            try {
                channel.basicNack(deliveryTag, false, true);
            } catch (IOException ignored) {
                // Fechar o channel também devolve entregas sem ack ao broker.
            }
        }
    }

    private void close(Channel channel, Connection connection) {
        try {
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
        } catch (Exception ignored) {
        }

        if (connection != null && connection.isOpen()) {
            connection.close();
        }
    }
}
