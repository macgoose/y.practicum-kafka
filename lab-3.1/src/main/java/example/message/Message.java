package example.message;

import java.time.Instant;

public record Message(
    Long senderId,
    Long recipientId,
    String message,
    Instant timestamp
) {
}
