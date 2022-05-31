package com.alex.kafka;

import com.alex.kafka.bean.ClickLog;
import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.Random;
import java.util.UUID;

@Component
public class ProducerThread implements CommandLineRunner {
    @Autowired
    private Producer producer;

    @Override
    public void run(String... args) throws Exception {
        System.out.println("The Runner start to initialize ...");
        try {
            for(int j =0 ; j<5000;j++){
              int batchSize = 10000;
                for(int i = 0 ; i<batchSize ;i++){
                    ClickLog clickLog  = new ClickLog();
                    clickLog.setId(UUID.randomUUID().toString());
                    clickLog.setClickTime(new Date());
                    clickLog.setType("webset");
                    clickLog.setUser("user"+ new Random().nextInt(1000) +i);
                    producer.sendMessage(Constant.topicName, JSONObject.toJSONString(clickLog));
                }
                Thread.sleep(50L);
                Constant.msgCount += batchSize;
                System.out.println("msgCount:"+Constant.msgCount);
            }
            System.out.println("msgCount:"+Constant.msgCount);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}