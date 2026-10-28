import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class MessageSingleConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        // Взять адреса брокеров из окружения; при запуске из IDE использовать localhost.
        props.put(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092,localhost:9093,localhost:9094")
        );
        // Объединить одиночные консюмеры в отдельную группу, независимо читающую топик.
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "single-group");
        // Преобразовывать ключ сообщения из байтов в строку.
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // Преобразовывать JSON сообщения в Java-объект.
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonDeserializer.class.getName());
        // Создавать объект Message при десериализации JSON.
        props.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, Message.class.getName());
        // Читать с начала доступных данных, если сохранённого offset нет или он недоступен.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Периодически сохранять offsets автоматически; это не коммит после каждой записи.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // Считать консюмер недоступным после 6 секунд отсутствия heartbeat на брокере.
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "6000");
        // Возвращать не больше одной записи за poll.
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);

        // Закрыть клиент при выходе из блока, в том числе из-за исключения.
        try (KafkaConsumer<String, Message> consumer = new KafkaConsumer<>(props)) {
            // Подписаться на топик; партиции распределяются между экземплярами одной группы.
            consumer.subscribe(Collections.singletonList("messages"));

            while (true) {
                ConsumerRecords<String, Message> records;

                try {
                    // Ждать результат до 100 мс; позиция чтения продвигается при получении записей.
                    records = consumer.poll(Duration.ofMillis(100));
                } catch (SerializationException ex) {
                    // Записать ошибку и повторить poll; повреждённая запись может вызвать её снова.
                    log.error("Cannot deserialize message", ex);
                    continue;
                }

                for (ConsumerRecord<String, Message> record : records) {
                    log.info(
                        "Message received: key = {}, value = {}, partition = {}, offset = {}",
                        record.key(),
                        record.value() == null ? null : record.value().getText(),
                        record.partition(),
                        record.offset()
                    );
                }
            }
        }
    }
}
