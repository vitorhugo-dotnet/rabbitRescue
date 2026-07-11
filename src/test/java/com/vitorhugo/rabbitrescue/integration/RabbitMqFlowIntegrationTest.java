package com.vitorhugo.rabbitrescue.integration;

import static com.vitorhugo.rabbitrescue.notification.messaging.RabbitNames.NOTIFICATIONS_DLQ;
import static com.vitorhugo.rabbitrescue.notification.messaging.RabbitNames.NOTIFICATIONS_QUEUE;
import static com.vitorhugo.rabbitrescue.notification.messaging.RabbitNames.NOTIFICATIONS_RETRY_QUEUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.vitorhugo.rabbitrescue.deadletter.DeadLetterResponse;
import com.vitorhugo.rabbitrescue.deadletter.DeadLetterService;
import com.vitorhugo.rabbitrescue.notification.api.CreateNotificationRequest;
import com.vitorhugo.rabbitrescue.notification.api.CreateNotificationResponse;
import com.vitorhugo.rabbitrescue.notification.application.NotificationService;
import com.vitorhugo.rabbitrescue.notification.application.NotificationStatusStore;
import com.vitorhugo.rabbitrescue.notification.domain.NotificationProcessingState;
import com.vitorhugo.rabbitrescue.notification.domain.NotificationStatus;
import com.vitorhugo.rabbitrescue.notification.domain.ProcessingMode;
import com.vitorhugo.rabbitrescue.provider.ProviderModeState;
import com.vitorhugo.rabbitrescue.support.TestcontainersConfiguration;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

/**
 * Testes ponta a ponta do fluxo RabbitMQ.
 *
 * Nada é mockado aqui:
 * - o RabbitMQ é real e sobe em Docker;
 * - exchanges, bindings, TTL e DLQ são declarados pela aplicação;
 * - o @RabbitListener consome de verdade;
 * - asserções aguardam o processamento assíncrono terminar.
 */
@SpringBootTest(properties = {
        // O teste usa Testcontainers, não o docker-compose.yml do desenvolvimento.
        "spring.docker.compose.enabled=false",

        // Mantém os testes rápidos sem alterar o valor usado pela aplicação.
        "rabbit-rescue.messaging.retry-delay=250ms",
        "rabbit-rescue.messaging.max-attempts=3",
        "rabbit-rescue.messaging.dead-letter-browse-limit=100",

        // Uma thread deixa a ordem dos retries previsível para o teste.
        "spring.rabbitmq.listener.simple.concurrency=1",
        "spring.rabbitmq.listener.simple.max-concurrency=1",
        "spring.rabbitmq.listener.simple.prefetch=1"
})
@Import(TestcontainersConfiguration.class)
@Execution(ExecutionMode.SAME_THREAD)
@DisplayName("Fluxo RabbitMQ com retry e DLQ")
class RabbitMqFlowIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(15);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(50);

    @Autowired
    private NotificationService notificationService;

    @Autowired
    private NotificationStatusStore statusStore;

    @Autowired
    private DeadLetterService deadLetterService;

    @Autowired
    private ProviderModeState providerModeState;

    @Autowired
    private AmqpAdmin amqpAdmin;

    @BeforeEach
    void prepareScenario() {
        providerModeState.changeTo(ProcessingMode.SUCCESS);
        purgeBrokerQueues();
        statusStore.clear();
    }

    @AfterEach
    void cleanScenario() {
        purgeBrokerQueues();
        statusStore.clear();
        providerModeState.changeTo(ProcessingMode.SUCCESS);
    }

    @Test
    @DisplayName("mensagem válida deve ser publicada, consumida e processada")
    void shouldPublishConsumeAndProcessValidMessage() {
        CreateNotificationResponse response = request(
                ProcessingMode.SUCCESS,
                "corr-success"
        );

        NotificationProcessingState state = awaitState(
                response.messageId(),
                NotificationStatus.PROCESSED
        );

        assertThat(state.attempt()).isEqualTo(1);
        assertThat(state.correlationId()).isEqualTo("corr-success");
        assertThat(deadLetterService.list(10)).isEmpty();
    }

    @Test
    @DisplayName("mensagem flaky deve falhar uma vez e preservar o correlationId no retry")
    void shouldRetryFlakyMessageAndPreserveCorrelationId() {
        CreateNotificationResponse response = request(
                ProcessingMode.FLAKY,
                "corr-flaky"
        );

        NotificationProcessingState state = awaitState(
                response.messageId(),
                NotificationStatus.PROCESSED
        );

        assertThat(state.attempt()).isEqualTo(2);
        assertThat(state.correlationId()).isEqualTo("corr-flaky");
        assertThat(state.lastError()).isNull();
        assertThat(deadLetterService.list(10)).isEmpty();
    }

    @Test
    @DisplayName("poison message deve ir diretamente para a DLQ")
    void shouldMoveInvalidMessageToDeadLetterQueue() {
        CreateNotificationResponse response = request(
                ProcessingMode.INVALID,
                "corr-invalid"
        );

        NotificationProcessingState state = awaitState(
                response.messageId(),
                NotificationStatus.DEAD_LETTERED
        );
        DeadLetterResponse deadLetter = awaitDeadLetter(response.messageId());

        assertThat(state.attempt()).isEqualTo(1);
        assertThat(deadLetter.correlationId()).isEqualTo("corr-invalid");
        assertThat(deadLetter.attempt()).isEqualTo(1);
        assertThat(deadLetter.errorType()).isEqualTo("PermanentProviderException");
        assertThat(deadLetter.errorMessage()).contains("Mensagem inválida");
        assertThat(deadLetter.payload().processingMode()).isEqualTo(ProcessingMode.INVALID);
    }

    @Test
    @DisplayName("falha transitória permanente deve respeitar o limite e terminar na DLQ")
    void shouldMoveMessageToDeadLetterQueueAfterRetryLimit() {
        CreateNotificationResponse response = request(
                ProcessingMode.FAIL,
                "corr-fail"
        );

        NotificationProcessingState state = awaitState(
                response.messageId(),
                NotificationStatus.DEAD_LETTERED
        );
        DeadLetterResponse deadLetter = awaitDeadLetter(response.messageId());

        assertThat(state.attempt()).isEqualTo(3);
        assertThat(state.correlationId()).isEqualTo("corr-fail");
        assertThat(deadLetter.attempt()).isEqualTo(3);
        assertThat(deadLetter.errorType()).isEqualTo("TransientProviderException");
        assertThat(deadLetter.errorMessage()).contains("Provider indisponível");
    }

    @Test
    @DisplayName("replay deve remover da DLQ, republicar e processar a mesma mensagem")
    void shouldReplayDeadLetterMessage() {
        /*
         * processingMode null faz o evento usar o modo global do provider.
         * Primeiro invalidamos para mandar à DLQ; depois mudamos para SUCCESS
         * antes do replay, simulando a correção da causa raiz.
         */
        providerModeState.changeTo(ProcessingMode.INVALID);

        CreateNotificationResponse response = request(
                null,
                "corr-replay"
        );

        awaitState(response.messageId(), NotificationStatus.DEAD_LETTERED);
        awaitDeadLetter(response.messageId());

        providerModeState.changeTo(ProcessingMode.SUCCESS);

        var replay = deadLetterService.replay(response.messageId());
        NotificationProcessingState processed = awaitState(
                response.messageId(),
                NotificationStatus.PROCESSED
        );

        assertThat(replay.messageId()).isEqualTo(response.messageId());
        assertThat(replay.correlationId()).isEqualTo("corr-replay");
        assertThat(replay.status()).isEqualTo("REPLAYED");
        assertThat(processed.attempt()).isEqualTo(1);
        assertThat(processed.correlationId()).isEqualTo("corr-replay");

        await().atMost(TIMEOUT)
                .pollInterval(POLL_INTERVAL)
                .untilAsserted(() ->
                        assertThat(deadLetterService.list(100))
                                .extracting(DeadLetterResponse::messageId)
                                .doesNotContain(response.messageId())
                );
    }

    private CreateNotificationResponse request(
            ProcessingMode processingMode,
            String correlationId
    ) {
        return notificationService.requestNotification(
                new CreateNotificationRequest(
                        List.of("integration@rabbitrescue.test"),
                        "Teste de integração",
                        "Mensagem processada pelo RabbitMQ real",
                        processingMode
                ),
                correlationId
        );
    }

    private NotificationProcessingState awaitState(
            UUID messageId,
            NotificationStatus expectedStatus
    ) {
        await().atMost(TIMEOUT)
                .pollInterval(POLL_INTERVAL)
                .untilAsserted(() ->
                        assertThat(statusStore.find(messageId))
                                .as("estado da mensagem %s", messageId)
                                .hasValueSatisfying(state ->
                                        assertThat(state.status()).isEqualTo(expectedStatus)
                                )
                );

        return statusStore.find(messageId).orElseThrow();
    }

    private DeadLetterResponse awaitDeadLetter(UUID messageId) {
        await().atMost(TIMEOUT)
                .pollInterval(POLL_INTERVAL)
                .until(() ->
                        deadLetterService.list(100).stream()
                                .anyMatch(message -> message.messageId().equals(messageId))
                );

        return deadLetterService.list(100).stream()
                .filter(message -> message.messageId().equals(messageId))
                .findFirst()
                .orElseThrow();
    }

    private void purgeBrokerQueues() {
        amqpAdmin.purgeQueue(NOTIFICATIONS_QUEUE);
        amqpAdmin.purgeQueue(NOTIFICATIONS_RETRY_QUEUE);
        amqpAdmin.purgeQueue(NOTIFICATIONS_DLQ);
    }
}
