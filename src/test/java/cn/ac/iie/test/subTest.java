package cn.ac.iie.test;

import cn.ac.iie.common.SubscriberConfig;
import com.zdjizhi.utils.StringUtil;

public class subTest {


    public static void main(String[] args) {
        boolean validation = dataValidation("1.168.40.123", "abcd");
        System.out.println(validation);

        if ("10,100,192".contains("1")){
            System.out.println("yes");
        }else{
            System.out.println("no");
        }

        String s = "192.168.40.125-abcd";
        System.out.println(s.split("-")[0]);
    }


    private static boolean dataValidation(String ip, String account) {
        if (StringUtil.isNotBlank(ip) && StringUtil.isNotBlank(account)) {
            String s = ip.split("\\.")[0];
            return !SubscriberConfig.CHECK_IP_SCOPE.contains(s);
        }
        return false;
    }

}
