package cn.edu.sdu.cs.starry.kafkaConsumer.dynamicConsumer.batch;

import cn.edu.sdu.cs.starry.kafkaConsumer.dynamicConsumer.BaseFetchOperator;
import cn.edu.sdu.cs.starry.kafkaConsumer.exception.ConsumerLogException;
import cn.edu.sdu.cs.starry.kafkaConsumer.log.IOffsetLogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;

/**
 * Message fetcher and LOG for batch
 *
 * @author SDU.xccui
 * @version 0.8.0
 */
public class BatchFetchOperator extends BaseFetchOperator {
    private static Logger LOG = LoggerFactory
            .getLogger(BatchFetchOperator.class);
    private Map<Integer, Long> consumeOffsetMap;


    public BatchFetchOperator(String topic, Set<Integer> managedPartitionSet, IOffsetLogManager logManager, String clientName)
            throws ConsumerLogException {
        super(topic, managedPartitionSet, logManager, clientName);
        consumeOffsetMap = Collections.synchronizedMap(new TreeMap());
    }

    /**
     * Acknowledge the consumed offset
     *
     * @param partitionId
     * @param offset
     */
    public void ackConsumeOffset(int partitionId, long offset) {
        consumeOffsetMap.put(partitionId, offset);
    }

    @Override
    public void loadHistoryOffsets() throws ConsumerLogException {
        for (Integer partitionId : managedPartitionSet) {
            consumeOffsetMap.put(partitionId, 0L);
        }
        logManager.loadOffsetLog(consumeOffsetMap);
        // Use consumeOffsetMap to initialize sendOffsetMap
        sendOffsetMap.putAll(consumeOffsetMap);
    }

    @Override
    public void loadHistoryOffsets(int partition) throws ConsumerLogException {
        consumeOffsetMap.put(partition,0L);
        logManager.loadOffsetLog(consumeOffsetMap);
        sendOffsetMap.putAll(consumeOffsetMap);
    }

    @Override
    public void flushOffsetAndRemovePartition(int partition) throws ConsumerLogException {
        Map<Integer, Long> offsetMap = new HashMap<>();
        offsetMap.put(partition,consumeOffsetMap.get(partition));
        logManager.saveOffsets(offsetMap);
        //remove partition from offset map
        consumeOffsetMap.remove(partition);
        sendOffsetMap.remove(partition);
    }

    @Override
    public void flushOffsets() throws ConsumerLogException {
        logManager.saveOffsets(Collections.unmodifiableMap(consumeOffsetMap));
    }

    @Override
    public void close() {
        LOG.info("BatchFetchOperator close\n=======Current offset map========\n");
        for (Entry<Integer, Long> entry : consumeOffsetMap.entrySet()) {
            LOG.info(entry.getKey() + ":" + entry.getValue());
        }
        logManager.close();
    }

    /**
     * Reset send offset to consumed offset
     */
    public void resetToConsumedOffset() {
        sendOffsetMap.putAll(consumeOffsetMap);
    }

    @Override
    public void handleLogError() {
        LOG.warn("BatchFetchOperator deals error");
        LOG.info("=======Current offset map=========\n");
        for (Entry<Integer, Long> entry : consumeOffsetMap.entrySet()) {
            LOG.warn(entry.getKey() + ":" + entry.getValue());
        }
        try {
            logManager.tryToReconnect();
        } catch (ConsumerLogException e) {
            LOG.error("Log manager reconnect error", e);
        }
    }

}
