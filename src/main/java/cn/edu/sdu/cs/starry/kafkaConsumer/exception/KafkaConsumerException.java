package cn.edu.sdu.cs.starry.kafkaConsumer.exception;

/**
 * Created by yestin on 2016/3/10.
 */
public class KafkaConsumerException extends Exception {
    public KafkaConsumerException() {
    }

    public KafkaConsumerException(String message) {
        super(message);
    }

    public KafkaConsumerException(String message, Throwable cause) {
        super(message, cause);
    }

    public KafkaConsumerException(Throwable cause) {
        super(cause);
    }

    public KafkaConsumerException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
