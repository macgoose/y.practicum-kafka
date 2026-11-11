package example;

import example.censorship.ForbiddenWords;
import example.message.BlockedUsersSerdes;
import example.message.CheckedMessage;
import example.message.Message;
import example.message.MessageSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;

import java.util.List;

public class MessageTopology {

    public static String MESSAGES_TOPIC = "messages";
    public static String FILTERED_MESSAGES_TOPIC = "filtered_messages";
    public static String BLOCKED_USERS_TOPIC = "blocked_users";
    public static String BLOCKED_USERS_STORE = "blocked-users-store";

    public Topology build(ForbiddenWords forbiddenWords) {
        StreamsBuilder sb = new StreamsBuilder();
        Serde<Message> messageSerdes = MessageSerdes.create();

        GlobalKTable<String, List<Long>> blockedUsers = sb.globalTable(
            BLOCKED_USERS_TOPIC,
            Consumed.with(Serdes.String(), BlockedUsersSerdes.create()),
            Materialized.as(Stores.persistentTimestampedKeyValueStore(BLOCKED_USERS_STORE))
        );

        KStream<String, Message> messages = sb
            .stream(MESSAGES_TOPIC, Consumed.with(Serdes.String(), messageSerdes))
            .leftJoin(
                blockedUsers,
                (messageKey, message) -> message.recipientId().toString(),
                (message, blockedForRecipient) -> new CheckedMessage(
                    message, blockedForRecipient != null && blockedForRecipient.contains(message.senderId())
                )
            )
            .filter((key, checked) -> !checked.blocked())
            .mapValues(CheckedMessage::message)
            .mapValues(forbiddenWords::censor);


        messages.to(FILTERED_MESSAGES_TOPIC, Produced.with(Serdes.String(), messageSerdes));

        return sb.build();
    }
}
