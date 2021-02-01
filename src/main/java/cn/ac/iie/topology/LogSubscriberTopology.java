package cn.ac.iie.topology;

import cn.ac.iie.bolt.SubscriberIdBolt;
import cn.ac.iie.common.SubscriberConfig;
import cn.ac.iie.spout.CustomizedKafkaSpout;
import org.apache.storm.Config;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author qidaijie
 */
public class LogSubscriberTopology {
    private static final Logger logger = LoggerFactory.getLogger(LogSubscriberTopology.class);
    private final String topologyName;
    private final Config topologyConfig;
    private TopologyBuilder builder;

    public LogSubscriberTopology() {
        this(LogSubscriberTopology.class.getSimpleName());
    }

    public LogSubscriberTopology(String topologyName) {
        this.topologyName = topologyName;
        topologyConfig = createTopologConfig();
    }

    private Config createTopologConfig() {
        Config conf = new Config();
        conf.setDebug(false);
        conf.setMessageTimeoutSecs(120);
        conf.setTopologyWorkerMaxHeapSize(500);
        conf.setMaxSpoutPending(SubscriberConfig.TOPOLOGY_CONFIG_MAX_SPOUT_PENDING);
        if (SubscriberConfig.TOPOLOGY_NUM_ACKS == 0) {
            conf.setNumAckers(0);
        }
        return conf;
    }

    public void runLocally() throws InterruptedException {
        topologyConfig.setMaxTaskParallelism(1);
        StormRunner.runTopologyLocally(builder, topologyName, topologyConfig, 600);
    }

    public void runRemotely() throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        topologyConfig.setNumWorkers(SubscriberConfig.TOPOLOGY_WORKERS);
        //设置过高会导致很多问题，如心跳线程饿死、吞吐量大幅下跌
        topologyConfig.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 8);
        StormRunner.runTopologyRemotely(builder, topologyName, topologyConfig);
    }

    private void buildTopology() {
        builder = new TopologyBuilder();
        builder.setSpout("CustomizedKafkaSpout", new CustomizedKafkaSpout(), SubscriberConfig.SPOUT_PARALLELISM);
        builder.setBolt("SubscriberIdBolt", new SubscriberIdBolt(), SubscriberConfig.FORMAT_BOLT_PARALLELISM).localOrShuffleGrouping("CustomizedKafkaSpout");
    }

    public static void main(String[] args) throws Exception {
        LogSubscriberTopology csst = null;
        boolean runLocally = true;
        if (args.length >= 2 && "remote".equalsIgnoreCase(args[1])) {
            runLocally = false;
            csst = new LogSubscriberTopology(args[0]);
        } else {
            csst = new LogSubscriberTopology();
        }
        csst.buildTopology();

        if (runLocally) {
            logger.info("执行本地模式...");
            csst.runLocally();
        } else {
            logger.info("执行远程部署模式...");
            csst.runRemotely();
        }
    }
}
