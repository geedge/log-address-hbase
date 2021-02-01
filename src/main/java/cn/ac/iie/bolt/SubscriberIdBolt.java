package cn.ac.iie.bolt;

import cn.ac.iie.common.SubscriberConfig;
import cn.ac.iie.utils.TupleUtils;
import com.alibaba.fastjson.JSONObject;
import com.zdjizhi.utils.StringUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.util.*;

/**
 * @author qidaijie
 */
public class SubscriberIdBolt extends BaseBasicBolt {
    private static Logger logger = Logger.getLogger(SubscriberIdBolt.class);
    private static Map<String, String> subIdMap;
    private List<Put> putList;
    private static Connection connection;

    static {
        // 管理Hbase的配置信息
        Configuration configuration = HBaseConfiguration.create();
        // 设置zookeeper节点
        configuration.set("hbase.zookeeper.quorum", SubscriberConfig.HBASE_ZOOKEEPER_SERVERS);
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        configuration.set("hbase.client.retries.number", "3");
        configuration.set("hbase.bulkload.retries.number", "3");
        configuration.set("zookeeper.recovery.retry", "3");
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            logger.error("Subscriber写入HBase程序连接HBase异常");
            e.printStackTrace();
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        subIdMap = new HashMap<>(3333334);
        putList = new ArrayList<>();
        getAll();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        try {
            if (TupleUtils.isTick(tuple)) {
                insertData(putList);
            } else {
                String message = tuple.getString(0);
                if (StringUtil.isNotBlank(message)) {
                    JSONObject jsonObject = JSONObject.parseObject(message);
                    if (jsonObject.containsKey(SubscriberConfig.PACKET_TYPE) && jsonObject.containsKey(SubscriberConfig.STATUS_TYPE)) {
                        if (SubscriberConfig.ACCOUNTING_REQUEST == jsonObject.getInteger(SubscriberConfig.PACKET_TYPE)
                                && SubscriberConfig.START_BILLING == jsonObject.getInteger(SubscriberConfig.STATUS_TYPE)) {
                            String framedIp = jsonObject.getString("radius_framed_ip");
                            String account = jsonObject.getString("radius_account");
                            dataValidation(framedIp, account, putList);
                        }
                        if (putList.size() == SubscriberConfig.LIST_SIZE_MAX) {
                            insertData(putList);
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Radius 日志解析/更新HBase 失败!");
            e.printStackTrace();
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>(16);
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,
                SubscriberConfig.TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        return conf;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    /**
     * 获取所有的 key value
     */
    private static void getAll() {
        try {
            Table table = connection.getTable(TableName.valueOf("sub:" + SubscriberConfig.HBASE_TABLE_NAME));
            Scan scan = new Scan();
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    subIdMap.put(Bytes.toString(CellUtil.cloneRow(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            logger.warn("初始化内存数据成功--初始化map大小->(" + subIdMap.size() + ")");
            scanner.close();
        } catch (IOException e) {
            logger.error("初始化内存数据异常");
            e.printStackTrace();
        }
    }

    /**
     * 写入数据到HBase
     *
     * @param putList puts list
     */
    private static void insertData(List<Put> putList) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf("sub:" + SubscriberConfig.HBASE_TABLE_NAME));
            table.put(putList);
            putList.clear();
            logger.warn("更新HBase成功，更新条数：" + putList.size());
        } catch (IOException e) {
            logger.error("更新数据写入HBase失败");
            e.printStackTrace();
        } finally {
            try {
                if (table != null) {
                    table.close();
                }
            } catch (IOException e) {
                logger.error("HBase表关闭异常");
                e.printStackTrace();
            }
        }

    }

    /**
     * 验证数据并与内存中的对比
     *
     * @param ip      framed_ip
     * @param account account
     */
    private static void dataValidation(String ip, String account, List<Put> putList) {
        if (subIdMap.containsKey(ip)) {
            if (!subIdMap.get(ip).equals(account)) {
                Put put = new Put(ip.getBytes());
                put.addColumn("subscriber_id".getBytes(), "account".getBytes(), account.getBytes());
                putList.add(put);
                subIdMap.put(ip, account);
            }
        } else {
            Put put = new Put(ip.getBytes());
            put.addColumn("subscriber_id".getBytes(), "account".getBytes(), account.getBytes());
            putList.add(put);
            subIdMap.put(ip, account);
        }
    }
}
