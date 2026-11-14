# Практическая работа 3 — аналитика сообщений в ksqlDB

Задание 2 использует отдельный брокер Kafka. В топик `messages` поступают JSON-сообщения с полями `user_id`, `recipient_id`, `message` и `timestamp`. Топик `user_statistics` предназначен для результатов агрегации.

## Запуск инфраструктуры

Команды выполняются из каталога `lab-3.2`. Сначала запускаются ZooKeeper и Kafka.

```fish
podman compose up -d zookeeper kafka-0
podman compose exec -T kafka-0 kafka-topics.sh --bootstrap-server kafka-0:9094 --list
```

После того как брокер отвечает на команду просмотра топиков, можно запустить ksqlDB Server и проверить его REST API.

```fish
podman compose up -d ksqldb-server
curl -fsS http://127.0.0.1:8088/info
```

Когда сервер готов, интерактивная консоль запускается по требованию.

```fish
podman compose run --rm --no-deps ksqldb-cli
```

## Создание топиков

Входящие сообщения:

```fish
podman compose exec -T kafka-0 kafka-topics.sh --bootstrap-server kafka-0:9094 --create --if-not-exists --topic messages --partitions 1 --replication-factor 1 --config cleanup.policy=delete
```

Результаты агрегации по пользователям:

```fish
podman compose exec -T kafka-0 kafka-topics.sh --bootstrap-server kafka-0:9094 --create --if-not-exists --topic user_statistics --partitions 1 --replication-factor 1 --config cleanup.policy=compact
```

## Загрузка тестовых данных

```fish
podman compose exec kafka-0 kafka-console-producer.sh --bootstrap-server kafka-0:9094 --topic messages
```

Каждая строка вводится как отдельная запись. Все поля находятся в JSON-значении. Время задано в UTC.

```jsonl
{"user_id":1,"recipient_id":2,"message":"Привет!","timestamp":"2026-11-14T10:00:00Z"}
{"user_id":1,"recipient_id":2,"message":"Есть минутка?","timestamp":"2026-11-14T10:01:00Z"}
{"user_id":1,"recipient_id":3,"message":"Как дела?","timestamp":"2026-11-14T10:02:00Z"}
{"user_id":2,"recipient_id":3,"message":"Доброе утро!","timestamp":"2026-11-14T10:03:00Z"}
```

Ввод завершается сочетанием **Ctrl+D**. На свежем стенде записи можно проверить консольным потребителем.

```fish
podman compose exec -T kafka-0 kafka-console-consumer.sh --bootstrap-server kafka-0:9094 --topic messages --from-beginning --max-messages 4
```

## Запуск SQL-запросов

В консоли `ksql>` последовательно выполняются команды из [ksqldb-queries.sql](ksqldb-queries.sql). Каждая команда заканчивается точкой с запятой. Скрипт рассчитан на первый запуск на стенде, где поток и таблицы ещё не созданы. Для уже настроенного стенда достаточно выполнить проверочные запросы ниже.

Настройка `auto.offset.reset = earliest` позволяет новым запросам учесть записи, загруженные до их запуска. Поток `messages_stream` описывает поля JSON в топике `messages`. Запросы `CREATE TABLE ... AS SELECT` продолжают обработку новых сообщений после закрытия CLI. Таблица `user_statistics` записывает результаты в одноимённый топик Kafka.

## Проверка статистики

```sql
SELECT total_messages FROM messages_totals WHERE scope = 'all';
SELECT unique_recipients FROM recipient_totals WHERE scope = 'all';

SELECT total_messages FROM messages_by_user WHERE user_id = 1;
SELECT total_messages FROM messages_by_user WHERE user_id = 2;

SELECT * FROM user_statistics WHERE user_id = 1;
SELECT * FROM user_statistics WHERE user_id = 2;
```

На четырёх тестовых записях общее количество сообщений равно **4**, а количество уникальных получателей равно **2**. В `messages_by_user` ожидаются счётчики **3** и **1** для пользователей `1` и `2` соответственно.

| `USER_ID` | `TOTAL_MESSAGES` | `UNIQUE_RECIPIENTS` |
|---:|---:|---:|
| 1 | 3 | 2 |
| 2 | 1 | 1 |

Если сразу после создания таблицы результат пуст или появляется сообщение `Materialized data ... is not available yet`, запрос следует повторить после обработки входных записей.

Для проверки обновлений в реальном времени можно открыть подписку в CLI:

```sql
SELECT * FROM user_statistics EMIT CHANGES;
```

Затем через консольный продюсер отправляется ещё одна запись в `messages`:

```json
{"user_id":1,"recipient_id":4,"message":"Новое сообщение","timestamp":"2026-11-14T10:04:00Z"}
```

После обработки новой записи общие показатели станут **5** и **3**, а статистика пользователя `1` обновится до `1 | 4 | 3`. Подписка завершается сочетанием **Ctrl+C**.
