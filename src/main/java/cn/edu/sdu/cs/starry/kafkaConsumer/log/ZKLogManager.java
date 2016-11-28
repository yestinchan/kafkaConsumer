package cn.edu.sdu.cs.starry.kafkaConsumer.log;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import cn.edu.sdu.cs.starry.kafkaConsumer.exception.ConsumerLogException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple zookeeper based log manager
 *
 * @author SDU.xccui
 */
public class ZKLogManager implements IOffsetLogManager, Watcher {
    private static Logger LOG = LoggerFactory.getLogger(ZKLogManager.class);
//    public static final String PATH_PREFIX = "/starry/kafkaConsumer/dynamic/";
    private static final int SESSION_TIME_OUT = 5000;
    private String zkHosts;
    private String zkBasePath;
    private ZooKeeper zooKeeper;
    private HashMap<Integer, String> partitionNodePathMap;

    public ZKLogManager(String prefix, String zkHosts, String consumerName, String topic)
            throws ConsumerLogException {
        if(!prefix.endsWith("/")) prefix+="/";
        zkBasePath = prefix + consumerName + "/" + topic;
        this.zkHosts = zkHosts;
        partitionNodePathMap = new HashMap<>();
        connect();
    }

    private void connect() throws ConsumerLogException {
        try {
            zooKeeper = new ZooKeeper(this.zkHosts, SESSION_TIME_OUT, this);
        } catch (IOException e) {
            LOG.error("connect error", e);
            throw new ConsumerLogException(e);
        }
    }

    private void createZKNode(String path, byte[] data) throws KeeperException,
            InterruptedException {
        String[] pathName = path.split("/");
        String subPath = "";
        for (int i = 0; i < pathName.length; i++) {
            if (pathName[i].length() > 0) {
                subPath += ("/" + pathName[i]);
                if (null == zooKeeper.exists(subPath, this)) {
                    zooKeeper.create(subPath, data, Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
                }
            }
        }
    }

    @Override
    public synchronized void loadOffsetLog(Map<Integer, Long> consumeOffsetMap)
            throws ConsumerLogException {
        try {
            LOG.info("zkBasePath: " + zkBasePath);
            Stat stat = zooKeeper.exists(zkBasePath, this);
            if (null == stat) {
                createZKNode(zkBasePath, null);
            }
            String partitionNodePath;
            Stat partitionNodeStat;
            for (Integer managedPartitionId : consumeOffsetMap.keySet()) {
                partitionNodePath = zkBasePath + "/" + managedPartitionId;
                partitionNodePathMap.put(managedPartitionId, partitionNodePath);
                partitionNodeStat = zooKeeper.exists(partitionNodePath, this);
                if (null == partitionNodeStat) {
                    createZKNode(partitionNodePath, "0".getBytes("utf-8"));
                } else {
                    try {
                        consumeOffsetMap.put(managedPartitionId, Long
                                .valueOf(new String(zooKeeper.getData(
                                        partitionNodePath, this,
                                        partitionNodeStat), "utf-8")));
                    } catch (Exception ex) {
                        LOG.error("exception while putting offset map ", ex);
                    }
                }
            }
        } catch (KeeperException e) {
            LOG.error("keeper exception ", e);
            throw new ConsumerLogException(e);
        } catch (InterruptedException e) {
            LOG.error("interrupted ", e);
            throw new ConsumerLogException(e);
        } catch (UnsupportedEncodingException e) {
            LOG.error("unexpected exception ", e);
            throw new ConsumerLogException(e);
        }
    }

    @Override
    public synchronized void saveOffsets(Map<Integer, Long> consumeOffsetMap)
            throws ConsumerLogException {
        for (Entry<Integer, Long> entry : consumeOffsetMap.entrySet()) {
            //TODO do not save all offsets including unchanged
            Stat stat;
            try {
                if (null == (stat = zooKeeper.exists(
                        partitionNodePathMap.get(entry.getKey()), this))) {
                    LOG.info("Create new offset node!\tpartitionId: "
                            + entry.getKey() + "\toffset: " + entry.getValue());
                    createZKNode(partitionNodePathMap.get(entry.getKey()),
                            "0".getBytes("utf-8"));
                }
                int oldVersion = stat.getVersion();
                stat = zooKeeper.setData(partitionNodePathMap.get(entry.getKey()),
                        String.valueOf(entry.getValue()).getBytes("utf-8"), stat.getVersion());
                LOG.info("offset writer version old:{}, new:{}, key:{}, value:{}",
                        oldVersion , stat.getVersion(), entry.getKey(), entry.getValue());

            } catch (KeeperException e) {
                LOG.error("keeper exception ", e);
                throw new ConsumerLogException(e);
            } catch (InterruptedException e) {
                LOG.error("intrrupted ", e);
                throw new ConsumerLogException(e);
            } catch (UnsupportedEncodingException e) {
                LOG.error("unexpected exception ", e);
                throw new ConsumerLogException(e);
            }
        }
    }

    @Override
    public synchronized void close() {
        try {
            zooKeeper.close();
        } catch (InterruptedException e) {
            LOG.error("interrupted ", e);
        }

    }

    @Override
    public void process(WatchedEvent event) {
        LOG.info("Process zk event type: " + event.getType());
        if (event.getState().equals(Event.KeeperState.Disconnected)) {
            close();
        }
    }

    public static void main(String[] args) throws ConsumerLogException {
        ZKLogManager manager = new ZKLogManager("/starry/kafkaConsumer/dynamic/","172.16.0.158:2181", "S4",
                "jnits");
        Map<Integer, Long> logMap = new HashMap<Integer, Long>();
        logMap.put(0, 0L);
        logMap.put(1, 0L);
        try {
            manager.loadOffsetLog(logMap);
        } catch (ConsumerLogException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        logMap.put(0, 200L);
        logMap.put(1, 201L);
        try {
            manager.saveOffsets(logMap);
            manager.close();
        } catch (ConsumerLogException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public void tryToReconnect() throws ConsumerLogException {
        try {
            zooKeeper.close();
        } catch (InterruptedException e) {
            LOG.error("interrupted ", e);
        }
        connect();
    }
}
