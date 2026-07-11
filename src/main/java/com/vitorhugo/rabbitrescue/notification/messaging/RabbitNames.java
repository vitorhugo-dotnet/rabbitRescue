package com.vitorhugo.rabbitrescue.notification.messaging;

/**
 * Nomes compartilhados da topologia RabbitMQ.
 *
 * Manter os valores em um único lugar evita o belo espetáculo de uma fila
 * publicar em um nome e o consumidor escutar outro.
 */
public final class RabbitNames {

    private RabbitNames() {
    }

    public static final String NOTIFICATIONS_EXCHANGE = "notifications.exchange";
    public static final String NOTIFICATIONS_ROUTING_KEY = "notifications.requested";
    public static final String NOTIFICATIONS_QUEUE = "notifications.queue";

    public static final String NOTIFICATIONS_DLX = "notifications.dlx";
    public static final String NOTIFICATIONS_RETRY_ROUTING_KEY = "notifications.retry";
    public static final String NOTIFICATIONS_DLQ_ROUTING_KEY = "notifications.dead-letter";
    public static final String NOTIFICATIONS_RETRY_QUEUE = "notifications.retry.queue";
    public static final String NOTIFICATIONS_DLQ = "notifications.dlq";

    public static final String HEADER_ATTEMPT = "x-attempt";
    public static final String HEADER_ERROR_TYPE = "x-error-type";
    public static final String HEADER_ERROR_MESSAGE = "x-error-message";
    public static final String HEADER_REPLAYED = "x-replayed";
}
