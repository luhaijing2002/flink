package com.xxxx.flink.test;

import com.xxxx.flink.util.KafkaUtil;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.Locale;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class tmp {
    public static void main(String[] args) {

        new Thread(() -> {
            String uname = RandomStringUtils.randomAlphabetic(8).toLowerCase(Locale.ROOT);
            for (int i = 100; i < 200; i++) {
                if (i % 10 == 0) {
                    KafkaUtil.sendMsg("test", uname + ":" + i + ":" +
                            (System.currentTimeMillis() - 15000L));
                } else if (i % 5 == 0) {
                    KafkaUtil.sendMsg("test", uname + ":" + i + ":" +
                            (System.currentTimeMillis() - 3000L));
                } else {
                    KafkaUtil.sendMsg("test", uname + ":" + i + ":" +
                            System.currentTimeMillis());
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();


    }
}
