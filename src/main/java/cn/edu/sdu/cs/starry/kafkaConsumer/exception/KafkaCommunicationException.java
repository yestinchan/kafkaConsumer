package cn.edu.sdu.cs.starry.kafkaConsumer.exception;

/**
 * @author xccui
 */
public class KafkaCommunicationException extends KafkaConsumerException {
    public KafkaCommunicationException(String message) {
        super(message);
    }

    public KafkaCommunicationException(Throwable throwable) {
        super(throwable);
    }
}
