package com.vitorhugo.rabbitrescue.notification.api;

import com.vitorhugo.rabbitrescue.notification.domain.ProcessingMode;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import java.util.List;

public record CreateNotificationRequest(
        @NotEmpty List<@NotBlank @Email String> recipients,
        @NotBlank String subject,
        @NotBlank String content,
        ProcessingMode processingMode
) {
    public CreateNotificationRequest {
        recipients = recipients == null ? null : List.copyOf(recipients);
    }
}
