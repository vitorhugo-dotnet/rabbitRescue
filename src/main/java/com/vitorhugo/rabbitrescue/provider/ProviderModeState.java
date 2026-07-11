package com.vitorhugo.rabbitrescue.provider;

import com.vitorhugo.rabbitrescue.notification.domain.ProcessingMode;
import java.util.concurrent.atomic.AtomicReference;
import org.springframework.stereotype.Component;

@Component
public class ProviderModeState {

    private final AtomicReference<ProcessingMode> currentMode =
            new AtomicReference<>(ProcessingMode.SUCCESS);

    public ProcessingMode current() {
        return currentMode.get();
    }

    public ProcessingMode changeTo(ProcessingMode mode) {
        currentMode.set(mode);
        return mode;
    }
}
