package cn.ac.iie.bolt;

import cn.ac.iie.common.AddressConfig;
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
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.util.*;

/**
 * @author qidaijie
 */
public class SubcribeIdBolt extends BaseBasicBolt {
    private static Logger logger = Logger.getLogger(SubcribeIdBolt.class);
    private static Map<String, String> subIdMap;
    private List<Put> putList;
    private static Connection connection;

    static {
        // 管理Hbase的配置信息
        Configuration configuration = HBaseConfiguration.create();
        // 设置zookeeper节点
        configuration.set("hbase.zookeeper.quorum", AddressConfig.HBASE_ZOOKEEPER_SERVERS);
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        subIdMap = new HashMap<>(16);
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
                    String ip = jsonObject.getString("framed_ip");
                    String account = jsonObject.getString("account");
                    dataValidation(ip, account, putList);
                    if (putList.size() == AddressConfig.LIST_SIZE_MAX) {
                        insertData(putList);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Radius写入Redis出现异常", e);
        }

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>(16);
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,
                AddressConfig.TOPOLOGY_TICK_TUPLE_FREQ_SECS);
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
            Table table = connection.getTable(TableName.valueOf("sub:" + AddressConfig.HBASE_TABLE_NAME));
            Scan scan2 = new Scan();
            ResultScanner scanner = table.getScanner(scan2);
            for (Result result : scanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    subIdMap.put(Bytes.toString(CellUtil.cloneRow(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            scanner.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 写入数据到hbase
     */
    private static void insertData(List<Put> putList) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf("sub:" + AddressConfig.HBASE_TABLE_NAME));
            table.put(putList);
            putList.clear();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (table != null) {
                    table.close();
                }
            } catch (IOException e) {
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
        if (StringUtil.isNotBlank(ip) && StringUtil.isNotBlank(account)) {
//            String s = ip.split("\\.")[0];
//            if (!AddressConfig.CHECK_IP_SCOPE.contains(s)) {
            if (subIdMap.containsKey(ip)) {
                if (!subIdMap.get(ip).equals(account)) {
                    Put put = new Put(ip.getBytes());
                    put.addColumn("subscribe_id".getBytes(), "account".getBytes(), account.getBytes());
                    putList.add(put);
                    subIdMap.put(ip, account);
                }
            } else {
                Put put = new Put(ip.getBytes());
                put.addColumn("subscribe_id".getBytes(), "account".getBytes(), account.getBytes());
                putList.add(put);
                subIdMap.put(ip, account);
            }
//            }
        }
    }
}
