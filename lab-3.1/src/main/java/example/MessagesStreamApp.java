package example;

import com.sun.net.httpserver.HttpServer;
import example.censorship.ForbiddenWords;
import example.censorship.ForbiddenWordsHttpServer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import java.io.IOException;
import java.util.Properties;

@Slf4j
public class MessagesStreamApp {

    public static void main(String[] args) throws IOException {
        // Конфигурация Kafka Streams
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "messages-streams-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        ForbiddenWords forbiddenWords = new ForbiddenWords();
        HttpServer server = ForbiddenWordsHttpServer.start(forbiddenWords, 8081);

        try {
            KafkaStreams streams = new KafkaStreams(new MessageTopology().build(forbiddenWords), properties);

            try {
                streams.start();
            } catch (RuntimeException e) {
                streams.close();
                throw e;
            }

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                server.stop(0);
                streams.close();
            }));

            log.info("Kafka Streams запускается. Словарь доступен через PUT http://127.0.0.1:8081/forbidden-words");
        } catch (RuntimeException e) {
            server.stop(0);
            throw e;
        }
    }
}
