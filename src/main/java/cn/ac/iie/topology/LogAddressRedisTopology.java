package cn.ac.iie.topology;

import cn.ac.iie.bolt.SubcribeIdBolt;
import cn.ac.iie.common.AddressConfig;
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
public class LogAddressRedisTopology {
    private static final Logger logger = LoggerFactory.getLogger(LogAddressRedisTopology.class);
    private final String topologyName;
    private final Config topologyConfig;
    private TopologyBuilder builder;

    public LogAddressRedisTopology() {
        this(LogAddressRedisTopology.class.getSimpleName());
    }

    public LogAddressRedisTopology(String topologyName) {
        this.topologyName = topologyName;
        topologyConfig = createTopologConfig();
    }

    private Config createTopologConfig() {
        Config conf = new Config();
        conf.setDebug(false);
        conf.setMessageTimeoutSecs(120);
        conf.setTopologyWorkerMaxHeapSize(500);
        conf.setMaxSpoutPending(AddressConfig.TOPOLOGY_CONFIG_MAX_SPOUT_PENDING);
        if (AddressConfig.TOPOLOGY_NUM_ACKS == 0) {
            conf.setNumAckers(0);
        }
        return conf;
    }

    public void runLocally() throws InterruptedException {
        topologyConfig.setMaxTaskParallelism(1);
        StormRunner.runTopologyLocally(builder, topologyName, topologyConfig, 600);
    }

    public void runRemotely() throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        topologyConfig.setNumWorkers(AddressConfig.TOPOLOGY_WORKERS);
        topologyConfig.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 8);//设置过高会导致很多问题，如心跳线程饿死、吞吐量大幅下跌
        StormRunner.runTopologyRemotely(builder, topologyName, topologyConfig);
    }

    private void buildTopology() {
        builder = new TopologyBuilder();
        builder.setSpout("CustomizedKafkaSpout", new CustomizedKafkaSpout(), AddressConfig.SPOUT_PARALLELISM);
        builder.setBolt("SubcribeIdBolt", new SubcribeIdBolt(), AddressConfig.FORMAT_BOLT_PARALLELISM).localOrShuffleGrouping("CustomizedKafkaSpout");
    }

    public static void main(String[] args) throws Exception {
        LogAddressRedisTopology csst = null;
        boolean runLocally = true;
        if (args.length >= 2 && "remote".equalsIgnoreCase(args[1])) {
            runLocally = false;
            csst = new LogAddressRedisTopology(args[0]);
        } else {
            csst = new LogAddressRedisTopology();
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
