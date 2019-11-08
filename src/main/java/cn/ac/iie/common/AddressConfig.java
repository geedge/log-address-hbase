package cn.ac.iie.common;

import cn.ac.iie.utils.AddressConfigurations;

import java.io.Serializable;

/**
 * @author qidaijie
 */
public class AddressConfig implements Serializable {
    private static final long serialVersionUID = -8326385159484059324L;

    public static final String SEGMENTATION = ",";
    public static final int LIST_SIZE_MAX = 5000;

    /***
     * kafka and system
     */
    public static final String BOOTSTRAP_SERVERS = AddressConfigurations.getStringProperty(0, "bootstrap.servers");
    public static final Integer SPOUT_PARALLELISM = AddressConfigurations.getIntProperty(0, "spout.parallelism");
    public static final Integer FORMAT_BOLT_PARALLELISM = AddressConfigurations.getIntProperty(0, "format.bolt.parallelism");
    public static final String GROUP_ID = AddressConfigurations.getStringProperty(0, "group.id");
    public static final String KAFKA_TOPIC = AddressConfigurations.getStringProperty(0, "kafka.topic");
    public static final String AUTO_OFFSET_RESET = AddressConfigurations.getStringProperty(0, "auto.offset.reset");
    public static final Integer TOPOLOGY_TICK_TUPLE_FREQ_SECS = AddressConfigurations.getIntProperty(0, "topology.tick.tuple.freq.secs");
    public static final Integer TOPOLOGY_CONFIG_MAX_SPOUT_PENDING = AddressConfigurations.getIntProperty(0, "topology.config.max.spout.pending");
    public static final Integer TOPOLOGY_NUM_ACKS = AddressConfigurations.getIntProperty(0, "topology.num.acks");

    public static final Integer TOPOLOGY_WORKERS = AddressConfigurations.getIntProperty(0, "topology.workers");

    public static final Integer EXPIRATION_TIME = AddressConfigurations.getIntProperty(0, "expiration.time");
    public static final Integer REDIS_MODEL = AddressConfigurations.getIntProperty(0, "redis.model");

    public static final String CHECK_IP_SCOPE = AddressConfigurations.getStringProperty(0, "check.ip.scope");

    public static final String HBASE_ZOOKEEPER_SERVERS = AddressConfigurations.getStringProperty(0, "hbase.zookeeper.servers");
    public static final String HBASE_TABLE_NAME = AddressConfigurations.getStringProperty(0, "hbase.table.name");
}