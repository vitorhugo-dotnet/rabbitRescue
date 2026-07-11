package com.vitorhugo.rabbitrescue.service;

import com.vitorhugo.rabbitrescue.bean.MessageBean;
import com.vitorhugo.rabbitrescue.config.RabbitMQConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class MessageService {

    private static final String ATTEMPT_HEADER = "x-attempt";
    private static final String FAILURE_REASON_HEADER = "x-failure-reason";

    /*
     * Total de processamentos permitidos:
     *
     * attempt 0 = processamento inicial
     * attempt 1 = primeiro retry
     * attempt 2 = segundo retry
     * attempt 3 = terceiro retry
     */
    private static final int MAX_ATTEMPTS = 3;

    private final RabbitTemplate rabbitTemplate;

    /**
     * Publica uma mensagem nova na exchange principal.
     */
    public void send(MessageBean message) {
        rabbitTemplate.convertAndSend(
                RabbitMQConfig.NOTIFICATIONS_EXCHANGE,
                RabbitMQConfig.NOTIFICATION_ROUTING_KEY,
                message,
                amqpMessage -> {
                    var properties = amqpMessage.getMessageProperties();

                    properties.setMessageId(message.id().toString());
                    properties.setCorrelationId(message.correlationId());
                    properties.setHeader(ATTEMPT_HEADER, 0);

                    return amqpMessage;
                }
        );

        log.info(
                "MessageBean published. messageId={}, correlationId={}, mode={}",
                message.id(),
                message.correlationId(),
                message.processingMode()
        );
    }

    /**
     * Consome mensagens da fila principal.
     * <p>
     * O header x-attempt informa quantas vezes a mensagem já foi reenviada.
     */
    @RabbitListener(queues = RabbitMQConfig.NOTIFICATIONS_QUEUE)
    public void consume(
            MessageBean message,
            @Header(value = ATTEMPT_HEADER, required = false) Integer attemptHeader
    ) {
        int attempt = attemptHeader == null ? 0 : attemptHeader;

        log.info(
                "Processing message. messageId={}, correlationId={}, mode={}, attempt={}",
                message.id(),
                message.correlationId(),
                message.processingMode(),
                attempt
        );

        try {
            simulateProvider(message, attempt);

            log.info(
                    "MessageBean processed successfully. messageId={}, correlationId={}, attempt={}",
                    message.id(),
                    message.correlationId(),
                    attempt
            );

        } catch (TransientMessageException exception) {
            handleTransientFailure(message, attempt, exception);

        } catch (InvalidMessageException exception) {
            sendToDeadLetterQueue(message, attempt, exception.getMessage());
        }
    }

    /**
     * Simula o serviço externo que processaria a mensagem.
     * <p>
     * Num sistema real, aqui poderia existir uma chamada HTTP,
     * envio de notificação, geração de relatório ou integração externa.
     */
    private void simulateProvider(MessageBean message, int attempt) {
        switch (message.processingMode()) {
            case SUCCESS -> processSuccessfully(message);

            case FAIL -> throw new TransientMessageException(
                    "Provider indisponível"
            );

            case FLAKY -> {
                /*
                 * Falha somente no primeiro processamento.
                 *
                 * Quando a mensagem voltar da fila de retry,
                 * attempt será maior que zero e ela será processada.
                 */
                if (attempt == 0) {
                    throw new TransientMessageException(
                            "Falha transitória simulada"
                    );
                }

                processSuccessfully(message);
            }

            case INVALID -> throw new InvalidMessageException(
                    "Mensagem inválida ou impossível de processar"
            );
        }
    }

    private void processSuccessfully(MessageBean message) {
        log.info(
                "Fake provider processed message. recipients={}, subject={}",
                message.recipients(),
                message.subject()
        );
    }

    /**
     * Falhas transitórias podem ser tentadas novamente.
     * <p>
     * Quando o limite é atingido, a mensagem é encaminhada à DLQ.
     */
    private void handleTransientFailure(
            MessageBean message,
            int attempt,
            TransientMessageException exception
    ) {
        if (attempt >= MAX_ATTEMPTS) {
            sendToDeadLetterQueue(
                    message,
                    attempt,
                    "Retry limit exceeded: " + exception.getMessage()
            );

            return;
        }

        sendToRetryQueue(message, attempt + 1, exception.getMessage());
    }

    /**
     * Envia a mensagem para a fila de retry.
     *
     * Essa fila deve possuir TTL e dead-letter exchange configurados
     * para devolver a mensagem à fila principal depois do atraso.
     */
    private void sendToRetryQueue(
            MessageBean message,
            int nextAttempt,
            String failureReason
    ) {
        rabbitTemplate.convertAndSend(
                RabbitMQConfig.NOTIFICATIONS_DLX,
                RabbitMQConfig.RETRY_ROUTING_KEY,
                message,
                amqpMessage -> {
                    var properties = amqpMessage.getMessageProperties();

                    properties.setMessageId(message.id().toString());
                    properties.setCorrelationId(message.correlationId());
                    properties.setHeader(ATTEMPT_HEADER, nextAttempt);
                    properties.setHeader(FAILURE_REASON_HEADER, failureReason);

                    return amqpMessage;
                }
        );

        log.warn(
                "MessageBean sent to retry queue. messageId={}, correlationId={}, nextAttempt={}, reason={}",
                message.id(),
                message.correlationId(),
                nextAttempt,
                failureReason
        );
    }

    /**
     * Envia mensagens inválidas ou que excederam o limite para a DLQ.
     */
    private void sendToDeadLetterQueue(
            MessageBean message,
            int attempt,
            String failureReason
    ) {
        rabbitTemplate.convertAndSend(
                RabbitMQConfig.NOTIFICATIONS_DLX,
                RabbitMQConfig.DEAD_LETTER_ROUTING_KEY,
                message,
                amqpMessage -> {
                    var properties = amqpMessage.getMessageProperties();

                    properties.setMessageId(message.id().toString());
                    properties.setCorrelationId(message.correlationId());
                    properties.setHeader(ATTEMPT_HEADER, attempt);
                    properties.setHeader(
                            FAILURE_REASON_HEADER,
                            failureReason
                    );

                    return amqpMessage;
                }
        );

        log.error(
                "MessageBean sent to DLQ. messageId={}, correlationId={}, attempt={}, reason={}",
                message.id(),
                message.correlationId(),
                attempt,
                failureReason
        );
    }

    /**
     * Representa uma falha que pode desaparecer numa próxima tentativa.
     */
    private static class TransientMessageException extends RuntimeException {

        private TransientMessageException(String message) {
            super(message);
        }
    }

    /**
     * Representa uma mensagem que nunca poderá ser processada corretamente.
     */
    private static class InvalidMessageException extends RuntimeException {

        private InvalidMessageException(String message) {
            super(message);
        }
    }
}