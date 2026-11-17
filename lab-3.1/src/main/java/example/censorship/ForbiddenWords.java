package example.censorship;

import example.message.Message;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

public final class ForbiddenWords {

    private static final Pattern WORD = Pattern.compile("[\\p{L}\\p{M}\\p{N}_]+");
    // HTTP-обработчик публикует новый неизменяемый словарь для потоков Kafka Streams.
    private volatile Set<String> words = Set.of();

    public void replace(List<String> newWords) {
        Set<String> normalized = new HashSet<>();
        for (String word : newWords) {
            String trimmed = word.strip();
            if (!WORD.matcher(trimmed).matches()) {
                throw new IllegalArgumentException("Ожидается список отдельных слов");
            }
            normalized.add(trimmed.toLowerCase(Locale.ROOT));
        }
        words = Set.copyOf(normalized);
    }

    public Message censor(Message message) {
        Set<String> snapshot = words;
        if (message.message() == null || snapshot.isEmpty()) {
            return message;
        }

        String censored = WORD.matcher(message.message()).replaceAll(match ->
            snapshot.contains(match.group().toLowerCase(Locale.ROOT)) ? "****" : match.group()
        );
        return new Message(message.senderId(), message.recipientId(), censored, message.timestamp());
    }
}
