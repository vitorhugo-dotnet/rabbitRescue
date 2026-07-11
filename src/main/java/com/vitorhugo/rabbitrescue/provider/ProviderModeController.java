package com.vitorhugo.rabbitrescue.provider;

import com.vitorhugo.rabbitrescue.notification.domain.ProcessingMode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/provider/mode")
public class ProviderModeController {

    private final ProviderModeState modeState;

    public ProviderModeController(ProviderModeState modeState) {
        this.modeState = modeState;
    }

    @PostMapping("/{mode}")
    public ResponseEntity<ProviderModeResponse> changeMode(@PathVariable String mode) {
        ProcessingMode selected = modeState.changeTo(ProcessingMode.fromPath(mode));
        return ResponseEntity.ok(new ProviderModeResponse(selected));
    }

    public record ProviderModeResponse(ProcessingMode mode) {
    }
}
