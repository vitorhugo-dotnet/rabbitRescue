package com.vitorhugo.rabbitrescue.config;

import static com.vitorhugo.rabbitrescue.notification.messaging.RabbitNames.NOTIFICATIONS_DLQ;
import static com.vitorhugo.rabbitrescue.notification.messaging.RabbitNames.NOTIFICATIONS_DLQ_ROUTING_KEY;
import static com.vitorhugo.rabbitrescue.notification.messaging.RabbitNames.NOTIFICATIONS_DLX;
import static com.vitorhugo.rabbitrescue.notification.messaging.RabbitNames.NOTIFICATIONS_EXCHANGE;
import static com.vitorhugo.rabbitrescue.notification.messaging.RabbitNames.NOTIFICATIONS_QUEUE;
import static com.vitorhugo.rabbitrescue.notification.messaging.RabbitNames.NOTIFICATIONS_RETRY_QUEUE;
import static com.vitorhugo.rabbitrescue.notification.messaging.RabbitNames.NOTIFICATIONS_RETRY_ROUTING_KEY;
import static com.vitorhugo.rabbitrescue.notification.messaging.RabbitNames.NOTIFICATIONS_ROUTING_KEY;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableRabbit
public class RabbitMQConfig {

    @Bean
    DirectExchange notificationsExchange() {
        return new DirectExchange(NOTIFICATIONS_EXCHANGE, true, false);
    }

    @Bean
    DirectExchange notificationsDeadLetterExchange() {
        return new DirectExchange(NOTIFICATIONS_DLX, true, false);
    }

    @Bean
    Queue notificationsQueue() {
        return QueueBuilder.durable(NOTIFICATIONS_QUEUE)
                // Proteção para exceções inesperadas do listener.
                .deadLetterExchange(NOTIFICATIONS_DLX)
                .deadLetterRoutingKey(NOTIFICATIONS_DLQ_ROUTING_KEY)
                .build();
    }

    @Bean
    Queue notificationsRetryQueue(MessagingProperties properties) {
        return QueueBuilder.durable(NOTIFICATIONS_RETRY_QUEUE)
                .ttl(Math.toIntExact(properties.retryDelay().toMillis()))
                // Quando o TTL expira, o broker devolve a mensagem ao fluxo principal.
                .deadLetterExchange(NOTIFICATIONS_EXCHANGE)
                .deadLetterRoutingKey(NOTIFICATIONS_ROUTING_KEY)
                .build();
    }

    @Bean
    Queue notificationsDeadLetterQueue() {
        return QueueBuilder.durable(NOTIFICATIONS_DLQ).build();
    }

    @Bean
    Binding notificationsBinding(
            @Qualifier("notificationsQueue") Queue notificationsQueue,
            @Qualifier("notificationsExchange") DirectExchange notificationsExchange
    ) {
        return BindingBuilder.bind(notificationsQueue)
                .to(notificationsExchange)
                .with(NOTIFICATIONS_ROUTING_KEY);
    }

    @Bean
    Binding retryBinding(
            @Qualifier("notificationsRetryQueue") Queue notificationsRetryQueue,
            @Qualifier("notificationsDeadLetterExchange") DirectExchange notificationsDeadLetterExchange
    ) {
        return BindingBuilder.bind(notificationsRetryQueue)
                .to(notificationsDeadLetterExchange)
                .with(NOTIFICATIONS_RETRY_ROUTING_KEY);
    }

    @Bean
    Binding deadLetterBinding(
            @Qualifier("notificationsDeadLetterQueue") Queue notificationsDeadLetterQueue,
            @Qualifier("notificationsDeadLetterExchange") DirectExchange notificationsDeadLetterExchange
    ) {
        return BindingBuilder.bind(notificationsDeadLetterQueue)
                .to(notificationsDeadLetterExchange)
                .with(NOTIFICATIONS_DLQ_ROUTING_KEY);
    }

    @Bean
    MessageConverter rabbitMessageConverter(ObjectMapper objectMapper) {
        return new Jackson2JsonMessageConverter(objectMapper);
    }
}
