package cn.edu.sdu.cs.starry.kafkaConsumer.dynamicConsumer.batch;

import cn.edu.sdu.cs.starry.kafkaConsumer.dynamicConsumer.BaseConsumer;
import cn.edu.sdu.cs.starry.kafkaConsumer.dynamicConsumer.ConsumerConfig;
import cn.edu.sdu.cs.starry.kafkaConsumer.exception.ConsumerConfigException;
import cn.edu.sdu.cs.starry.kafkaConsumer.exception.ConsumerLogException;
import cn.edu.sdu.cs.starry.kafkaConsumer.exception.KafkaErrorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;

/**
 * Batch consumer with ack.
 *
 * @author SDU.xccui
 * @version 0.8.0
 */
public class BatchConsumer extends BaseConsumer {
    private static Logger LOG = LoggerFactory.getLogger(BatchConsumer.class);

    private Object ackLock = new Object();

    public BatchConsumer(ConsumerConfig consumerConfig, String consumerName, String topic, Set<Integer> managedPartitionsSet)
            throws ConsumerConfigException, ConsumerLogException {
        super(consumerConfig, consumerName, topic, managedPartitionsSet);
    }

    public BatchConsumer(String consumerName, String topic, Set<Integer> managedPartitionsSet)
            throws ConsumerConfigException, ConsumerLogException {
        super(consumerName, topic, managedPartitionsSet);
    }

    public void prepare() throws ConsumerConfigException, ConsumerLogException {
        super.prepare();
        LOG.info("Finished initializing, then set batchOffset");
        try {
            setBatchOffsets();
        } catch (KafkaErrorException e) {
            e.printStackTrace();
            LOG.error("Can not initialize offsets, will exit.", e);
            System.exit(1);
        }
        LOG.info("Set batchOffset finished");
    }

    @Override
    protected void initFetchOperator() throws ConsumerLogException {
        Set<Integer> partitionSet = new HashSet();
        partitionSet.addAll(managedPartitionsSet);
        fetchOperator = new BatchFetchOperator(topic,
                managedPartitionsSet,
                logManager, consumerName);
    }

    /**
     * Acknowledge the consumed offset after consumed.
     *
     * @param ackMap
     */
    public void ackMessage(Map<Integer, Long> ackMap) {
        synchronized (ackLock) {
            for (Entry<Integer, Long> entry : ackMap.entrySet()) {
                ((BatchFetchOperator) fetchOperator).ackConsumeOffset(
                        entry.getKey(), entry.getValue());
            }
            try {
                fetchOperator.flushOffsets();
            } catch (ConsumerLogException e) {
                LOG.warn("Flush offsets error!");
                LOG.warn(e.getMessage());
                fetchOperator.handleLogError();
            }
        }
    }

    @Override
    public void close() {
        // do nothing
        fetchOperator.close();  //by sry
    }

    public void resetToConsumedOffset() {
        ((BatchFetchOperator) fetchOperator).resetToConsumedOffset();
    }
}
