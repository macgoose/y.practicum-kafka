package example.message;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.io.IOException;
import java.util.List;

public final class BlockedUsersSerdes {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final TypeReference<List<Long>> BLOCKED_USERS_TYPE = new TypeReference<>() {};

    public static Serde<List<Long>> create() {
        return Serdes.serdeFrom(BlockedUsersSerdes::serialize, BlockedUsersSerdes::deserialize);
    }

    private static byte[] serialize(String topic, List<Long> blockedUsers) {
        if (blockedUsers == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsBytes(blockedUsers);
        } catch (JsonProcessingException e) {
            throw new SerializationException("Ошибка сериализации списка блокировок", e);
        }
    }

    private static List<Long> deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return objectMapper.readValue(data, BLOCKED_USERS_TYPE);
        } catch (IOException e) {
            throw new SerializationException("Ошибка десериализации списка блокировок", e);
        }
    }
}
