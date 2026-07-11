package com.vitorhugo.rabbitrescue.bean;

public enum ProcessingMode {

    /**
     * A mensagem será processada normalmente.
     */
    SUCCESS,

    /**
     * Simula uma falha permanente.
     * Após atingir o limite de tentativas, a mensagem deve ir para a DLQ.
     */
    FAIL,

    /**
     * Simula uma falha transitória.
     * A primeira tentativa falha e a próxima deve funcionar.
     */
    FLAKY,

    /**
     * Representa uma mensagem inválida, também chamada de poison message.
     * Não faz sentido tentar processá-la novamente.
     */
    INVALID
}