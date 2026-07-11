package com.vitorhugo.rabbitrescue.bean;


import java.util.List;
import java.util.UUID;

public record MessageBean (
        UUID id,
        String correlationId,
        List<String> recipients,
        String subject,
        String content,
        ProcessingMode processingMode
) {}