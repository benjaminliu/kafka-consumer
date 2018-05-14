package com.example.demo.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

public class Listener {
    private static Logger logger = LoggerFactory.getLogger(Listener.class);

    @KafkaListener(topics = "${topic}")
    public void listen(ConsumerRecord<?, ?> cr, Acknowledgment acknowledgment) throws Exception {
        logger.info("Thread:{} - msg{}", Thread.currentThread().getName(), cr.value().toString());
        acknowledgment.acknowledge();
    }
}
