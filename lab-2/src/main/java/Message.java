import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

// Сгенерировать getter для чтения поля и сериализации в JSON.
@Getter
public class Message {

    private final String text;

    // Восстановить сообщение из поля text входящего JSON.
    @JsonCreator
    public Message(@JsonProperty("text") String text) {
        this.text = text;
    }

    // Создать исходящее сообщение с текущим локальным временем процесса.
    public Message() {
        String dateTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        this.text = "message-" + dateTime;
    }
}
