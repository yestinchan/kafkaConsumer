package cn.edu.sdu.cs.starry.kafkaConsumer.exception;

/**
 * Created by yestin on 2016/3/14.
 */
public class PartitionNotInException extends KafkaConsumerException {
    public PartitionNotInException() {
    }

    public PartitionNotInException(String message) {
        super(message);
    }

    public PartitionNotInException(String message, Throwable cause) {
        super(message, cause);
    }

    public PartitionNotInException(Throwable cause) {
        super(cause);
    }

    public PartitionNotInException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
