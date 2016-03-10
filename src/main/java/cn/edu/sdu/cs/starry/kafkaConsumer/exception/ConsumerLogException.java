package cn.edu.sdu.cs.starry.kafkaConsumer.exception;

/**
 * 
 * @author SDU.xccui
 * 
 */
public class ConsumerLogException extends KafkaConsumerException {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4177851257625270648L;

	public ConsumerLogException(String message) {
		super(message);
	}

	public ConsumerLogException(Throwable throwable) {
		super(throwable);
	}
}
