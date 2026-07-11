package com.vitorhugo.rabbitrescue.service;

import com.vitorhugo.rabbitrescue.bean.MessageBean;
import com.vitorhugo.rabbitrescue.config.RabbitMQConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class MessageService {
    private final RabbitTemplate rabbitTemplate;

    public void sendMessage(MessageBean message) {
        log.info("Sending message to exchange: {}, routingKey: {}, message: {}", RabbitMQConfig.NOTIFICATIONS_EXCHANGE, RabbitMQConfig.NOTIFICATION_ROUTING_KEY, message);
        rabbitTemplate.convertAndSend(RabbitMQConfig.NOTIFICATIONS_EXCHANGE, RabbitMQConfig.NOTIFICATION_ROUTING_KEY, message);
    }

    @RabbitListener(queues = RabbitMQConfig.NOTIFICATIONS_QUEUE)
    public void receiveMessage(MessageBean message) {
        log.info("Received message from queue: {}, message: {}", RabbitMQConfig.NOTIFICATIONS_QUEUE, message);
    }

    private void process(MessageBean message) {

    }
}
