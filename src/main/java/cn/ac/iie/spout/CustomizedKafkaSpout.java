package cn.ac.iie.spout;

import cn.ac.iie.common.SubscriberConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * @author qidaijie
 */
public class CustomizedKafkaSpout extends BaseRichSpout {
    private static final long serialVersionUID = 2934528972182398950L;
    private KafkaConsumer<String, String> consumer;
    private SpoutOutputCollector collector = null;
    private TopologyContext context = null;
    private final static Logger logger = Logger.getLogger(CustomizedKafkaSpout.class);


    private static Properties createConsumerConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", SubscriberConfig.BOOTSTRAP_SERVERS);
        props.put("group.id", SubscriberConfig.GROUP_ID);
        props.put("auto.offset.reset", SubscriberConfig.AUTO_OFFSET_RESET);
        props.put("session.timeout.ms", "60000");
        props.put("max.poll.records", 3000);
        props.put("max.partition.fetch.bytes", 31457280);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        // TODO Auto-generated method stub
        this.collector = collector;
        this.context = context;
        Properties prop = createConsumerConfig();
        this.consumer = new KafkaConsumer<>(prop);
        this.consumer.subscribe(Arrays.asList(SubscriberConfig.KAFKA_TOPIC));
    }

    @Override
    public void close() {
        consumer.close();
    }

    @Override
    public void nextTuple() {
        try {
            // TODO Auto-generated method stub
            ConsumerRecords<String, String> records = consumer.poll(10000L);
            Thread.sleep(300);
            for (ConsumerRecord<String, String> record : records) {
                this.collector.emit(new Values(record.value()));
            }
        } catch (Exception e) {
            logger.error("kfaka-spout 发送数据出现异常" + e);
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub
        declarer.declare(new Fields("source"));
    }
}
