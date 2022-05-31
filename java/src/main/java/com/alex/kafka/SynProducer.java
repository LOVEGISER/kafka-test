package com.alex.kafka;

import com.alex.kafka.bean.ClickLog;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
/**
 * 同步发送消息
 */
 
public class SynProducer {
    private static Properties getProps(){
        Properties props =  new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");
        props.put("acks", "all"); // 发送所有ISR
//        props.put("retries", 2); // 重试次数
//        props.put("batch.size", 16384); // 批量发送大小
//        props.put("buffer.memory", 102400); // 缓存大小，根据本机内存大小配置
//        props.put("linger.ms", 1000); // 发送频率，满足任务一个条件发送
        props.put("client.id", "producer-syn"); // 发送端id,便于统计
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
 
    public void send() {
        KafkaProducer<String, String> producer = new KafkaProducer<>(getProps());
        String strDateFormat = "yyyy-MM-dd HH:mm:ss";
        try {
            for(int j =0 ; j<50000;j++){
                int batchSize = 1000;
                for(int i = 0 ; i<batchSize ;i++){
                    ClickLog clickLog  = new ClickLog();
                    clickLog.setId(UUID.randomUUID().toString());
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(strDateFormat);
                    clickLog.setClickTime(simpleDateFormat.format(new Date()));
                    clickLog.setType("webset");
                    clickLog.setUser("user"+ new Random().nextInt(1000) +i);
                    // 三个参数，topic,key：用户分配partition,value:发送的值
                    ProducerRecord<String, String> record = new ProducerRecord<>(Constant.topicName,JSONObject.toJSONString(clickLog));
                    Future<RecordMetadata> metadataFuture = producer.send(record);
                    RecordMetadata recordMetadata = null;
                    try {
                        recordMetadata = metadataFuture.get();

                    } catch (InterruptedException|ExecutionException e) {

                        e.printStackTrace();
                    }
                }
                Thread.sleep(100L);
                Constant.msgCount += batchSize;
                System.out.println("msgCount:"+Constant.msgCount);
            }
            System.out.println("msgCount:"+Constant.msgCount);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }



        producer.flush();
        producer.close();
    }
}