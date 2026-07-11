package com.vitorhugo.rabbitrescue.deadletter;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import java.util.List;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Validated
@RestController
@RequestMapping("/notifications/dead-letters")
public class DeadLetterController {

    private final DeadLetterService deadLetterService;

    public DeadLetterController(DeadLetterService deadLetterService) {
        this.deadLetterService = deadLetterService;
    }

    @GetMapping
    public ResponseEntity<List<DeadLetterResponse>> list(
            @RequestParam(defaultValue = "50") @Min(1) @Max(1000) int limit
    ) {
        return ResponseEntity.ok(deadLetterService.list(limit));
    }

    @PostMapping("/{messageId}/replay")
    public ResponseEntity<ReplayResponse> replay(@PathVariable UUID messageId) {
        return ResponseEntity.accepted().body(deadLetterService.replay(messageId));
    }
}
