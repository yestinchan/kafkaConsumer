package cn.edu.sdu.cs.starry.kafkaConsumer.exception;

/**
 * 
 * @author SDU.xccui
 * 
 */
public class ConsumerConfigException extends KafkaConsumerException {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5272672264264948415L;

	public ConsumerConfigException(String message) {
		super(message);
	}
}
