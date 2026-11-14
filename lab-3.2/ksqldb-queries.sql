SET 'auto.offset.reset' = 'earliest';

CREATE STREAM messages_stream (
    user_id BIGINT,
    recipient_id BIGINT,
    message VARCHAR,
    timestamp VARCHAR
) WITH (
    KAFKA_TOPIC = 'messages',
    VALUE_FORMAT = 'JSON'
);

CREATE TABLE messages_totals AS
SELECT 'all' AS scope, COUNT(*) AS total_messages
FROM messages_stream
GROUP BY 'all'
EMIT CHANGES;

CREATE TABLE recipient_totals AS
SELECT
    'all' AS scope,
    ARRAY_LENGTH(COLLECT_SET(recipient_id)) AS unique_recipients
FROM messages_stream
GROUP BY 'all'
EMIT CHANGES;

CREATE TABLE messages_by_user AS
SELECT user_id, COUNT(*) AS total_messages
FROM messages_stream
GROUP BY user_id
EMIT CHANGES;

CREATE TABLE user_statistics
WITH (KAFKA_TOPIC = 'user_statistics', VALUE_FORMAT = 'JSON')
AS
SELECT
    user_id,
    COUNT(*) AS total_messages,
    ARRAY_LENGTH(COLLECT_SET(recipient_id)) AS unique_recipients
FROM messages_stream
GROUP BY user_id
EMIT CHANGES;
