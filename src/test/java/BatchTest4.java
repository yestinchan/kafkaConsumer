import cn.edu.sdu.cs.starry.kafkaConsumer.dynamicConsumer.batch.BatchConsumer;
import cn.edu.sdu.cs.starry.kafkaConsumer.entity.KafkaMessage;
import cn.edu.sdu.cs.starry.kafkaConsumer.exception.ConsumerConfigException;
import cn.edu.sdu.cs.starry.kafkaConsumer.exception.ConsumerLogException;
import cn.edu.sdu.cs.starry.kafkaConsumer.exception.KafkaCommunicationException;
import cn.edu.sdu.cs.starry.kafkaConsumer.exception.PartitionAlreadyInException;

import java.util.*;

/**
 * Created by yestin on 2016/3/10.
 */
public class BatchTest4 {
    public static void main(String[] args) throws ConsumerConfigException, ConsumerLogException, KafkaCommunicationException, PartitionAlreadyInException {
        Set<Integer> managedSet = new HashSet<>();
        managedSet.add(0);
        BatchConsumer consumer = new BatchConsumer("test4","test",managedSet);
        int i = 0;
        while(true){
            List<KafkaMessage> fetchedMessages = consumer.fetchMessage(1024);
            for(KafkaMessage message: fetchedMessages){
                System.out.println(String.format("id:[%s],offset:[%s],content:[%s]",
                        message.getPartitionId(), message.getOffset(), new String(message.getMessage())));
                Map<Integer, Long> ackMap = new HashMap<>();
                ackMap.put(message.getPartitionId(), message.getOffset());
                consumer.ackMessage(ackMap);
            }
            i ++;
            if( i== 10){
                consumer.addConsumePartition(1);
            } else if (i == 15){
                consumer.addConsumePartition(2);
            } else if (i == 20){
                consumer.addConsumePartition(1);
            }
        }
    }
}
