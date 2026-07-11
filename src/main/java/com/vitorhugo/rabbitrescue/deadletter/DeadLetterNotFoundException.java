package com.vitorhugo.rabbitrescue.deadletter;

import java.util.UUID;

public class DeadLetterNotFoundException extends RuntimeException {

    public DeadLetterNotFoundException(UUID messageId) {
        super("Mensagem " + messageId + " não encontrada na DLQ");
    }
}
