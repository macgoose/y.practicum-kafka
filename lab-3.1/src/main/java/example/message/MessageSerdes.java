package example.message;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class MessageSerdes {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static Serde<Message> create() {
        return Serdes.serdeFrom(MessageSerdes::serialize, MessageSerdes::deserialize);
    }

    public static Message deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return objectMapper.readValue(data, Message.class);
        } catch (Exception e) {
            throw new RuntimeException("Ошибка десериализации объекта Message", e);
        }
    }

    public static byte[] serialize(String topic, Message data) {
        if (data == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Ошибка сериализации объекта Message", e);
        }
    }
}
