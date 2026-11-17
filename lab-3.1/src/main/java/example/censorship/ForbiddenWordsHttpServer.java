package example.censorship;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public final class ForbiddenWordsHttpServer {

    private static final String PATH = "/forbidden-words";
    private static final ObjectMapper MAPPER = new ObjectMapper()
        .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    public static HttpServer start(ForbiddenWords forbiddenWords, int port) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        server.createContext(PATH, exchange -> handle(exchange, forbiddenWords));
        server.start();
        return server;
    }

    private static void handle(HttpExchange exchange, ForbiddenWords forbiddenWords) throws IOException {
        try (exchange) {
            if (!PATH.equals(exchange.getRequestURI().getPath())) {
                exchange.sendResponseHeaders(404, -1);
                return;
            }
            if (!"PUT".equals(exchange.getRequestMethod())) {
                exchange.getResponseHeaders().set("Allow", "PUT");
                exchange.sendResponseHeaders(405, -1);
                return;
            }

            try {
                forbiddenWords.replace(readWords(exchange));
            } catch (IOException | IllegalArgumentException e) {
                byte[] error = "Ожидается JSON-массив отдельных слов, например [\"спам\"]"
                    .getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
                exchange.sendResponseHeaders(400, error.length);
                exchange.getResponseBody().write(error);
                return;
            }
            exchange.sendResponseHeaders(204, -1);
        }
    }

    private static List<String> readWords(HttpExchange exchange) throws IOException {
        JsonNode body = MAPPER.readTree(exchange.getRequestBody());
        if (body == null || !body.isArray()) {
            throw new IllegalArgumentException();
        }

        List<String> words = new ArrayList<>();
        for (JsonNode entry : body) {
            if (!entry.isTextual()) {
                throw new IllegalArgumentException();
            }
            words.add(entry.textValue());
        }
        return words;
    }
}
