package com.vitorhugo.rabbitrescue.config;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Configurações que alteram o comportamento do fluxo de retry e DLQ.
 *
 * @param retryDelay tempo que a mensagem permanece na fila de retry
 * @param maxAttempts quantidade total de processamentos, incluindo o primeiro
 * @param deadLetterBrowseLimit limite de segurança ao percorrer a DLQ
 */
@Validated
@ConfigurationProperties(prefix = "rabbit-rescue.messaging")
public record MessagingProperties(
        @NotNull Duration retryDelay,
        @Min(1) @Max(20) int maxAttempts,
        @Min(1) @Max(1000) int deadLetterBrowseLimit
) {
}
