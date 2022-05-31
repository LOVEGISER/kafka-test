package com.alex.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Service
public class Consumer {
    private static final Logger log = LoggerFactory.getLogger(Consumer.class);

    @KafkaListener(topics = {Constant.topicName} )
    public void processMessage(String message) {
        log.debug("received message,thread id:[{}] {} {}",Thread.currentThread().getId(),"TopicA",message);
    }

}
