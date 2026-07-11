package com.vitorhugo.rabbitrescue.notification.application;

import com.vitorhugo.rabbitrescue.notification.domain.NotificationProcessingState;
import com.vitorhugo.rabbitrescue.notification.domain.NotificationStatus;
import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.springframework.stereotype.Component;

/**
 * Estado observável em memória usado pelo MVP e pelos testes assíncronos.
 *
 * Em produção, este componente seria substituído por persistência ou métricas.
 */
@Component
public class NotificationStatusStore {

    private final ConcurrentMap<UUID, NotificationProcessingState> states =
            new ConcurrentHashMap<>();
    private final Clock clock;

    public NotificationStatusStore() {
        this(Clock.systemUTC());
    }

    NotificationStatusStore(Clock clock) {
        this.clock = clock;
    }

    /**
     * Registra a publicação inicial sem sobrescrever um estado mais novo.
     *
     * O consumer pode ser rápido o suficiente para concluir ou mandar a
     * mensagem à DLQ antes de requestNotification chamar este método.
     * Portanto, qualquer estado já existente deve ser preservado.
     */
    public void markPublished(UUID messageId, String correlationId) {
        states.putIfAbsent(
                messageId,
                publishedState(messageId, correlationId)
        );
    }

    /**
     * Reinicia explicitamente o acompanhamento de uma mensagem reenfileirada.
     *
     * Diferente da publicação inicial, o replay pode substituir somente o
     * estado DEAD_LETTERED. Estados PROCESSING ou PROCESSED continuam intocados
     * para não perder uma atualização produzida pelo consumer.
     */
    public void markReplayPublished(UUID messageId, String correlationId) {
        states.compute(messageId, (id, current) -> {
            if (current != null
                    && current.status() != NotificationStatus.DEAD_LETTERED) {
                return current;
            }

            return publishedState(messageId, correlationId);
        });
    }

    public void update(
            UUID messageId,
            String correlationId,
            NotificationStatus status,
            int attempt,
            String lastError
    ) {
        states.put(messageId, new NotificationProcessingState(
                messageId,
                correlationId,
                status,
                attempt,
                lastError,
                Instant.now(clock)
        ));
    }

    public Optional<NotificationProcessingState> find(UUID messageId) {
        return Optional.ofNullable(states.get(messageId));
    }

    public void clear() {
        states.clear();
    }

    private NotificationProcessingState publishedState(
            UUID messageId,
            String correlationId
    ) {
        return new NotificationProcessingState(
                messageId,
                correlationId,
                NotificationStatus.PUBLISHED,
                1,
                null,
                Instant.now(clock)
        );
    }
}
