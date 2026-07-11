package com.vitorhugo.rabbitrescue.notification.application;

import static org.assertj.core.api.Assertions.assertThat;

import com.vitorhugo.rabbitrescue.notification.domain.NotificationStatus;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Protege a sincronização entre a thread HTTP e o consumer assíncrono.
 */
class NotificationStatusStoreTest {

    @Test
    void initialPublishMustNotOverwriteADeadLetterStateCreatedByTheConsumer() {
        NotificationStatusStore store = new NotificationStatusStore();
        UUID messageId = UUID.randomUUID();

        store.update(
                messageId,
                "corr-race",
                NotificationStatus.DEAD_LETTERED,
                1,
                "Mensagem inválida"
        );

        // Simula requestNotification chamando markPublished atrasado.
        store.markPublished(messageId, "corr-race");

        assertThat(store.find(messageId))
                .hasValueSatisfying(state ->
                        assertThat(state.status())
                                .isEqualTo(NotificationStatus.DEAD_LETTERED)
                );
    }

    @Test
    void replayMayExplicitlyMoveADeadLetterBackToPublished() {
        NotificationStatusStore store = new NotificationStatusStore();
        UUID messageId = UUID.randomUUID();

        store.update(
                messageId,
                "corr-replay",
                NotificationStatus.DEAD_LETTERED,
                3,
                "Limite excedido"
        );

        store.markReplayPublished(messageId, "corr-replay");

        assertThat(store.find(messageId))
                .hasValueSatisfying(state -> {
                    assertThat(state.status())
                            .isEqualTo(NotificationStatus.PUBLISHED);
                    assertThat(state.attempt()).isEqualTo(1);
                });
    }
}
