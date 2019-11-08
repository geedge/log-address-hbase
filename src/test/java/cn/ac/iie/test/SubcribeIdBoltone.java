package cn.ac.iie.test;

import cn.ac.iie.common.AddressConfig;
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
import java.util.HashMap;
import java.util.Map;

/**
 * @author qidaijie
 */
public class SubcribeIdBoltone extends BaseBasicBolt {
    private static Logger logger = Logger.getLogger(SubcribeIdBoltone.class);
    private static Map<String, String> subIdMap;
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
        getAll();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        try {
            String message = tuple.getString(0);
            if (StringUtil.isNotBlank(message)) {
                JSONObject jsonObject = JSONObject.parseObject(message);
                String ip = jsonObject.getString("framed_ip");
                String account = jsonObject.getString("account");
//                dataValidation(ip, account, collector);
                collector.emit(new Values(ip + "-" + account));
            }
        } catch (Exception e) {
            logger.error("Radius写入Redis出现异常", e);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("connLog"));
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
     * 验证数据并与内存中的对比
     *
     * @param ip      framed_ip
     * @param account account
     */
    private static void dataValidation(String ip, String account, BasicOutputCollector collector) {
        if (StringUtil.isNotBlank(ip) && StringUtil.isNotBlank(account)) {
//            String s = ip.split("\\.")[0];
//            if (!AddressConfig.CHECK_IP_SCOPE.contains(s)) {
            if (subIdMap.containsKey(ip)) {
                if (!subIdMap.get(ip).equals(account)) {
                    subIdMap.put(ip, account);
                    collector.emit(new Values(ip + "-" + account));
                }
            } else {
                subIdMap.put(ip, account);
                collector.emit(new Values(ip + "-" + account));
            }
//            }
        }
    }
}
