package cn.edu.sdu.cs.starry.kafkaConsumer.exception;

/**
 * Created by yestin on 2016/3/10.
 */
public class PartitionAlreadyInException extends KafkaConsumerException {
    public PartitionAlreadyInException() {
    }

    public PartitionAlreadyInException(String message) {
        super(message);
    }

    public PartitionAlreadyInException(String message, Throwable cause) {
        super(message, cause);
    }

    public PartitionAlreadyInException(Throwable cause) {
        super(cause);
    }

    public PartitionAlreadyInException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
