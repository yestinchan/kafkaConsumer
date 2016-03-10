import cn.edu.sdu.cs.starry.kafkaConsumer.dynamicConsumer.stream.IMessageSender;
import cn.edu.sdu.cs.starry.kafkaConsumer.dynamicConsumer.stream.StreamConsumer;
import cn.edu.sdu.cs.starry.kafkaConsumer.entity.KafkaMessage;
import cn.edu.sdu.cs.starry.kafkaConsumer.exception.ConsumerConfigException;
import cn.edu.sdu.cs.starry.kafkaConsumer.exception.ConsumerLogException;
import cn.edu.sdu.cs.starry.kafkaConsumer.exception.KafkaCommunicationException;
import cn.edu.sdu.cs.starry.kafkaConsumer.exception.PartitionAlreadyInException;

import java.util.HashSet;
import java.util.Set;

public class Test3 {
    public static void main(String args[]) throws ConsumerConfigException, ConsumerLogException, KafkaCommunicationException {
        Set<Integer> managedSet = new HashSet<Integer>();
//        for(int i=0;i<30;i++){
//            managedSet.add(i);
//        }
//        managedSet.remove(23);
//        managedSet.remove(8);
        managedSet.add(0);
        final StreamConsumer consumer = new StreamConsumer("sry", "test", managedSet,new IMessageSender() {
            @Override
            public void sendMessage(KafkaMessage message) throws Exception {
               /*byte[] bs = message.getMessage();
               Message msg = Message.fromBytes(bs);
               final byte[] text = MessageDivider.getText(msg);
               final byte[] image = MessageDivider.getImage(msg);
               String recstr = new String(text, "UTF-8");
               System.out.println(recstr);
               System.out.println(image.length);
               FileOutputStream stream = new FileOutputStream(new File("F:/test.jpg"));
               stream.write(image);
               stream.flush();
               stream.close();
               Thread.sleep(5000);*/
                byte[] bs = message.getMessage();
                System.out.println(new String(bs,"utf-8"));
            }

            @Override
            public void close() {
                //To change body of implemented methods use File | Settings | File Templates.
            }
        });
         new Thread(){
            @Override
            public void run() {
                super.run();
                try {
                    sleep(10000);
                    System.out.println("=============wake up===============");
                    consumer.addConsumePartition(1);
                    System.out.println("===============sleep====================");
                    sleep(2000);
                    System.out.println("===============wakeup====================");
                    consumer.addConsumePartition(2);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KafkaCommunicationException e) {
                    e.printStackTrace();
                } catch (ConsumerLogException e) {
                    e.printStackTrace();
                } catch (PartitionAlreadyInException e) {
                    e.printStackTrace();
                }
            }
        }.start();
        consumer.startFetchingAndPushing(false, 10 * 1024 * 1024, 500);

    }
}

