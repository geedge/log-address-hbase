package cn.ac.iie.utils;

import org.apache.storm.Constants;
import org.apache.storm.tuple.Tuple;

/**
 * @author Administrator
 */
public final class TupleUtils {
    /**
     * 判断是否系统自动发送的Tuple
     *
     * @param tuple
     * @return
     */
    public static boolean isTick(Tuple tuple) {
        return tuple != null
                && Constants.SYSTEM_COMPONENT_ID.equals(tuple.getSourceComponent())
                && Constants.SYSTEM_TICK_STREAM_ID.equals(tuple.getSourceStreamId());
    }
}
