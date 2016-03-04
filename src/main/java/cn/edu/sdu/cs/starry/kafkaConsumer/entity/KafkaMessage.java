package cn.edu.sdu.cs.starry.kafkaConsumer.entity;

/**
 * Contains a byte array for messages, a partitionId and an offset
 *
 * @author SDU.xccui
 * @version 0.8.0
 */
public class KafkaMessage {
    private final byte[] message;
    private final long offset;
    private final int partitionId;

    public KafkaMessage(byte[] message, int partitionId, long offset) {
        this.message = message;
        this.partitionId = partitionId;
        this.offset = offset;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public byte[] getMessage() {
        return message;
    }

    public long getOffset() {
        return offset;
    }

    public String toString() {
        return "KafkaMessage [partitionId = " + partitionId + " offset = " + offset + " message = " + new String(message) + "]";
    }

}
