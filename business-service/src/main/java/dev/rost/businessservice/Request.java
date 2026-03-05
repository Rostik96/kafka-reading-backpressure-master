package dev.rost.businessservice;

import java.time.Instant;

public record Request(
        String messageId,
        String payload,
        Instant producedAt
) {}
