package com.vitorhugo.rabbitrescue.notification.domain;

public enum NotificationStatus {
    PUBLISHED,
    PROCESSING,
    RETRY_SCHEDULED,
    PROCESSED,
    DEAD_LETTERED
}
