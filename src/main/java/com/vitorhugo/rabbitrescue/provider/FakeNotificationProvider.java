package com.vitorhugo.rabbitrescue.provider;

import com.vitorhugo.rabbitrescue.notification.domain.NotificationRequested;
import com.vitorhugo.rabbitrescue.notification.domain.ProcessingMode;
import org.springframework.stereotype.Component;

/**
 * Provider previsível para demonstrar os quatro caminhos do fluxo.
 */
@Component
public class FakeNotificationProvider {

    private final ProviderModeState modeState;

    public FakeNotificationProvider(ProviderModeState modeState) {
        this.modeState = modeState;
    }

    public void send(NotificationRequested notification, int attempt) {
        ProcessingMode mode = notification.processingMode() != null
                ? notification.processingMode()
                : modeState.current();

        switch (mode) {
            case SUCCESS -> {
                // Processamento concluído.
            }
            case FAIL -> throw new TransientProviderException(
                    "Provider indisponível temporariamente"
            );
            case FLAKY -> {
                if (attempt == 1) {
                    throw new TransientProviderException(
                            "Falha transitória simulada na primeira tentativa"
                    );
                }
            }
            case INVALID -> throw new PermanentProviderException(
                    "Mensagem inválida para o provider"
            );
        }
    }
}
