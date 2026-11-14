package example.message;

public record CheckedMessage(Message message, boolean blocked) {}