import cn.edu.sdu.cs.starry.kafkaConsumer.dynamicConsumer.stream.IMessageSender;
import cn.edu.sdu.cs.starry.kafkaConsumer.dynamicConsumer.stream.StreamConsumer;
import cn.edu.sdu.cs.starry.kafkaConsumer.entity.KafkaMessage;
import cn.edu.sdu.cs.starry.kafkaConsumer.exception.ConsumerConfigException;
import cn.edu.sdu.cs.starry.kafkaConsumer.exception.ConsumerLogException;
import cn.edu.sdu.cs.starry.kafkaConsumer.exception.KafkaCommunicationException;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by yestin on 2016/3/4.
 */
public class Test2 {
    public static void main(String[] args) throws ConsumerConfigException, ConsumerLogException, KafkaCommunicationException {

        Set<Integer> managedSet = new HashSet<Integer>();
        managedSet.add(0);

        StreamConsumer consumer = new StreamConsumer("test", "jnits", managedSet, new IMessageSender() {
            @Override
            public void sendMessage(KafkaMessage message) throws Exception {
                System.out.println("Got off: "+ message.getOffset());
            }

            @Override
            public void close() {

            }
        });
        consumer.startFetchingAndPushing(true, 10*1024*1024,500);

    }
}
