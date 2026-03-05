package dev.rost.kafkareader;

import java.time.Instant;

record Request(
        String messageId,
        String payload,
        Instant producedAt
) {}
