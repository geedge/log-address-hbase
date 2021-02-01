package cn.ac.iie.utils;

import java.util.Properties;



public final class SubscriberConfigurations {

    private static Properties propCommon = new Properties();
//    private static Properties propService = new Properties();


    public static String getStringProperty(Integer type, String key) {
        if (type == 0) {
            return propCommon.getProperty(key);
//        } else if (type == 1) {
//            return propService.getProperty(key);
        } else {
            return null;
        }
    }


    public static Integer getIntProperty(Integer type, String key) {
        if (type == 0) {
            return Integer.parseInt(propCommon.getProperty(key));
//        } else if (type == 1) {
//            return Integer.parseInt(propService.getProperty(key));
        } else {
            return null;
        }
    }

    public static Long getLongProperty(Integer type, String key) {
        if (type == 0) {
            return Long.parseLong(propCommon.getProperty(key));
//        } else if (type == 1) {
//            return Long.parseLong(propService.getProperty(key));
        } else {
            return null;
        }
    }

    public static Boolean getBooleanProperty(Integer type, String key) {
        if (type == 0) {
            return "true".equals(propCommon.getProperty(key).toLowerCase().trim());
//        } else if (type == 1) {
//            return propService.getProperty(key).toLowerCase().trim().equals("true");
        } else {
            return null;
        }
    }

    static {
        try {
            propCommon.load(SubscriberConfigurations.class.getClassLoader().getResourceAsStream("subscriber-config.properties"));
        } catch (Exception e) {
            propCommon = null;
            System.err.println("配置加载失败");
        }
    }
}
