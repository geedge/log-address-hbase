package cn.ac.iie.test;

import cn.ac.iie.common.SubscriberConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggerTest {
    private static final Logger logger = LoggerFactory.getLogger(LoggerTest.class);

    public static void main(String[] args) {
        try{
            int a = 1;
            throw new Exception();
        }catch (Exception e){
            int b = 20;
            logger.error("id is {} aaaaaaaaaa {}.",SubscriberConfig.KAFKA_TOPIC,b,e);
        }


    }
}
