# Практическая работа 3 — обработка сообщений в Kafka Streams

`MessagesStreamApp` запускает Kafka Streams на хосте и подключается к брокеру по `localhost:9092`. `MessageTopology` читает `messages`, проверяет блокировки по получателю, маскирует запрещённые слова и пишет разрешённые сообщения в `filtered_messages`. Списки из `blocked_users` материализуются в локальном персистентном хранилище `blocked-users-store`.

## Развёртывание

Podman Compose запускает Kafka, ZooKeeper и Kafka UI. Java-приложение запускается отдельно на хосте из IntelliJ IDEA. Для запуска требуется JDK 17. Каталог `kafka-stream` следует открыть как Maven-проект по файлу `pom.xml`.

Все команды выполняются из каталога `kafka-stream`. Сначала запускается инфраструктура и проверяется доступность брокера.

```fish
podman compose up -d
podman compose exec -T kafka-0 kafka-topics.sh --bootstrap-server kafka-0:9094 --list
```

Внутри сети Compose брокер доступен по адресу `kafka-0:9094`. Приложение на хосте подключается по `localhost:9092`. После создания топиков точка входа `example.MessagesStreamApp` запускается из IntelliJ IDEA. Для проверки блокировок тестовый список из раздела 4 загружается до запуска приложения.

## 1. Создать топики

Топик для входящих сообщений

```fish
podman compose exec -T kafka-0 kafka-topics.sh --bootstrap-server kafka-0:9094 --create --if-not-exists --topic messages --partitions 1 --replication-factor 1 --config cleanup.policy=delete
```

Топик для обработанных сообщений

```fish
podman compose exec -T kafka-0 kafka-topics.sh --bootstrap-server kafka-0:9094 --create --if-not-exists --topic filtered_messages --partitions 1 --replication-factor 1 --config cleanup.policy=delete
```

Топик для списков блокировок

```fish
podman compose exec -T kafka-0 kafka-topics.sh --bootstrap-server kafka-0:9094 --create --if-not-exists --topic blocked_users --partitions 1 --replication-factor 1 --config cleanup.policy=compact
```

## 2. Отправить тестовые сообщения

Порядок проверки фильтрации

1. Загрузить списки блокировок по инструкции из раздела 4.
2. Запустить `example.MessagesStreamApp` из IntelliJ IDEA и дождаться состояния `RUNNING`.
3. Загрузить словарь по HTTP по инструкции из раздела 3. После ответа `204` отправить тестовые сообщения.

```fish
podman compose exec kafka-0 kafka-console-producer.sh --bootstrap-server kafka-0:9094 --topic messages --property parse.key=true --property key.separator='|'
```

Записи вводятся по одной на строку. Число перед `|` — ключ Kafka. В примерах оно совпадает с ID получателя. Для ручного прогона поле `timestamp` оставлено пустым значением `null`.

```text
2|{"senderId":1,"recipientId":2,"message":"Привет, как дела?","timestamp":null}
3|{"senderId":1,"recipientId":3,"message":"Привет!","timestamp":null}
2|{"senderId":3,"recipientId":2,"message":"Это спам","timestamp":null}
```

Для завершения работы продюсера используется **Ctrl+D**. Результат можно проверить в отдельном терминале.

```fish
podman compose exec -T kafka-0 kafka-console-consumer.sh --bootstrap-server kafka-0:9094 --topic filtered_messages --from-beginning --property print.key=true
```

После загрузки блокировки из раздела 4 сообщение `1 → 2` отфильтруется, а `1 → 3` и `3 → 2` пройдут. После загрузки словаря из раздела 3 сообщение `3 → 2` будет содержать «Это ****». При повторном прогоне консюмер с `--from-beginning` покажет также записи прошлых запусков. Для остановки консюмера используется **Ctrl+C**.

## 3. Загрузить запрещённые слова по HTTP

`ForbiddenWordsHttpServer` принимает JSON-массив слов. Каждый успешный PUT целиком заменяет словарь `ForbiddenWords` в памяти приложения. Эндпоинт доступен локально на хосте.

```fish
curl --fail-with-body -i -X PUT http://127.0.0.1:8081/forbidden-words -H 'Content-Type: application/json' --data '["спам"]'
```

Успешный запрос возвращает `204 No Content`. Некорректное тело возвращает `400 Bad Request`. Цензура заменяет целые слова на `****` без учёта регистра. Например, «СПАМ!» превращается в «****!», а «спамер» сохраняется.

Для проверки динамического обновления словаря используется следующий PUT-запрос к работающему приложению.

```fish
curl --fail-with-body -i -X PUT http://127.0.0.1:8081/forbidden-words -H 'Content-Type: application/json' --data '["спам","реклама"]'
```

Затем в `messages` отправляется запись `2|{"senderId":3,"recipientId":2,"message":"Это реклама!","timestamp":null}`. В `filtered_messages` ожидается «Это ****!». Для очистки списка в теле PUT-запроса передаётся `[]`. После перезапуска приложения словарь нужно загрузить снова.

## 4. Отправить списки блокировок

```fish
podman compose exec kafka-0 kafka-console-producer.sh --bootstrap-server kafka-0:9094 --topic blocked_users --property parse.key=true --property key.separator='|'
```

Пример записи, в которой пользователь `2` блокирует отправителя `1`.

```text
2|[1]
```

Ключ — ID получателя, значение — полный JSON-массив заблокированных отправителей. Запись `2|[]` очищает его список. Изменения списка проверяются на новых сообщениях. Для завершения работы продюсера используется **Ctrl+D**.
