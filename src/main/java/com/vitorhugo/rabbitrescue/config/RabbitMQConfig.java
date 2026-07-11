package com.vitorhugo.rabbitrescue.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configura toda a topologia RabbitMQ utilizada pelo RabbitRescue.
 * <p>
 * Fluxo principal:
 * <p>
 * notifications.exchange
 *      ↓ notifications.requested
 * notifications.queue
 * <p>
 * Falha transitória:
 * <p>
 * notifications.dlx
 *      ↓ notifications.retry
 * notifications.retry.queue
 *      ↓ após o TTL
 * notifications.exchange
 * <p>
 * Falha definitiva:
 * <p>
 * notifications.dlx
 *      ↓ notifications.dead
 * notifications.dlq
 */
@Configuration
public class RabbitMQConfig {

    /*
     * Exchange principal responsável por receber novas notificações
     * e mensagens que retornaram da fila de retry.
     */
    public static final String NOTIFICATIONS_EXCHANGE =
            "notifications.exchange";

    /*
     * Dead Letter Exchange.
     *
     * Recebe mensagens que precisam ir para retry ou para a DLQ.
     */
    public static final String NOTIFICATIONS_DLX =
            "notifications.dlx";

    /*
     * Fila principal consumida pelo NotificationWorker.
     */
    public static final String NOTIFICATIONS_QUEUE =
            "notifications.queue";

    /*
     * Fila temporária usada para atrasar uma nova tentativa.
     *
     * Após o TTL expirar, a mensagem retorna para o exchange principal.
     */
    public static final String NOTIFICATIONS_RETRY_QUEUE =
            "notifications.retry.queue";

    /*
     * Dead Letter Queue.
     *
     * Armazena mensagens inválidas ou que excederam
     * o número máximo de tentativas.
     */
    public static final String NOTIFICATIONS_DLQ =
            "notifications.dlq";

    /*
     * Routing key usada para publicar novas notificações
     * no exchange principal.
     */
    public static final String NOTIFICATION_ROUTING_KEY =
            "notifications.requested";

    /*
     * Routing key usada para encaminhar falhas transitórias
     * para a fila de retry.
     */
    public static final String RETRY_ROUTING_KEY =
            "notifications.retry";

    /*
     * Routing key usada para encaminhar falhas definitivas
     * para a DLQ.
     */
    public static final String DEAD_LETTER_ROUTING_KEY =
            "notifications.dead";

    /*
     * Header customizado que registra quantas tentativas
     * de processamento já foram realizadas.
     */
    public static final String ATTEMPT_HEADER =
            "x-attempt";

    /*
     * Header customizado que registra o motivo
     * da última falha de processamento.
     */
    public static final String FAILURE_REASON_HEADER =
            "x-failure-reason";

    /*
     * Quantidade máxima de retries.
     *
     * O processamento inicial não é contado como retry.
     */
    public static final int MAX_RETRY_ATTEMPTS =
            3;

    /*
     * Tempo, em milissegundos, que a mensagem permanece
     * na fila de retry antes de voltar para a fila principal.
     */
    public static final int RETRY_DELAY_MS =
            5_000;

    /**
     * Exchange principal das notificações.
     *
     * durable = true:
     * o exchange continua existindo após reinicialização do RabbitMQ.
     *
     * autoDelete = false:
     * o exchange não será removido automaticamente quando não houver bindings.
     */
    @Bean
    public DirectExchange notificationsExchange() {
        return new DirectExchange(
                NOTIFICATIONS_EXCHANGE,
                true,
                false
        );
    }

    /**
     * Exchange responsável pelo roteamento de mensagens
     * para retry ou DLQ.
     */
    @Bean
    public DirectExchange notificationsDeadLetterExchange() {
        return new DirectExchange(
                NOTIFICATIONS_DLX,
                true,
                false
        );
    }

    /**
     * Fila principal consumida pelo worker.
     *
     * Caso uma mensagem seja rejeitada sem requeue,
     * ela será enviada ao Dead Letter Exchange usando
     * a routing key de falha definitiva.
     */
    @Bean
    public Queue notificationsQueue() {
        return QueueBuilder
                .durable(NOTIFICATIONS_QUEUE)
                .deadLetterExchange(NOTIFICATIONS_DLX)
                .deadLetterRoutingKey(DEAD_LETTER_ROUTING_KEY)
                .build();
    }

    /**
     * Fila responsável por aplicar atraso entre tentativas.
     *
     * A mensagem permanece nesta fila durante o período definido
     * em app.rabbitmq.retry-delay-ms.
     *
     * Quando o TTL expira, o RabbitMQ encaminha automaticamente
     * a mensagem novamente para o exchange principal.
     */
    @Bean
    public Queue notificationsRetryQueue(
            @Value("${app.rabbitmq.retry-delay-ms:5000}")
            int retryDelayMs
    ) {
        return QueueBuilder
                .durable(NOTIFICATIONS_RETRY_QUEUE)

                // Define quanto tempo a mensagem ficará aguardando.
                .ttl(retryDelayMs)

                // Após o TTL, a mensagem volta ao exchange principal.
                .deadLetterExchange(NOTIFICATIONS_EXCHANGE)

                // Routing key usada para retornar à fila principal.
                .deadLetterRoutingKey(NOTIFICATION_ROUTING_KEY)

                .build();
    }

    /**
     * Fila de mensagens irrecuperáveis.
     *
     * Não possui TTL nem outro DLX porque as mensagens devem permanecer
     * disponíveis para análise ou replay manual.
     */
    @Bean
    public Queue notificationsDeadLetterQueue() {
        return QueueBuilder
                .durable(NOTIFICATIONS_DLQ)
                .build();
    }

    /**
     * Liga o exchange principal à fila principal.
     *
     * Toda mensagem publicada com notifications.requested
     * será encaminhada para notifications.queue.
     */
    @Bean
    public Binding notificationsBinding(
            @Qualifier("notificationsQueue")
            Queue queue,

            @Qualifier("notificationsExchange")
            DirectExchange exchange
    ) {
        return BindingBuilder
                .bind(queue)
                .to(exchange)
                .with(NOTIFICATION_ROUTING_KEY);
    }

    /**
     * Liga o Dead Letter Exchange à fila de retry.
     *
     * O worker deve publicar falhas transitórias no DLX
     * usando a routing key notifications.retry.
     */
    @Bean
    public Binding notificationsRetryBinding(
            @Qualifier("notificationsRetryQueue")
            Queue retryQueue,

            @Qualifier("notificationsDeadLetterExchange")
            DirectExchange deadLetterExchange
    ) {
        return BindingBuilder
                .bind(retryQueue)
                .to(deadLetterExchange)
                .with(RETRY_ROUTING_KEY);
    }

    /**
     * Liga o Dead Letter Exchange à DLQ.
     *
     * Mensagens publicadas ou rejeitadas com notifications.dead
     * serão armazenadas em notifications.dlq.
     */
    @Bean
    public Binding notificationsDeadLetterBinding(
            @Qualifier("notificationsDeadLetterQueue")
            Queue deadLetterQueue,

            @Qualifier("notificationsDeadLetterExchange")
            DirectExchange deadLetterExchange
    ) {
        return BindingBuilder
                .bind(deadLetterQueue)
                .to(deadLetterExchange)
                .with(DEAD_LETTER_ROUTING_KEY);
    }
}