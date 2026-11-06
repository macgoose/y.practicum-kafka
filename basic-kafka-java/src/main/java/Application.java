public class Application {

    public static void main(String[] args) {
        // Запустить три независимых цикла в отдельных потоках внутри одного экземпляра приложения.
        // Ошибка одного потока пока не завершает остальные.
        new Thread(() -> MessageProducer.main(args), "message-producer").start();
        new Thread(() -> MessageSingleConsumer.main(args), "single-message-consumer").start();
        new Thread(() -> MessageBatchConsumer.main(args), "batch-message-consumer").start();
    }
}
