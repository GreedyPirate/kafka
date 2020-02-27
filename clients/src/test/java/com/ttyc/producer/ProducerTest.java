package com.ttyc.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerTest {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("linger.ms", 100);
        props.put("acks", "all");


        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 10000; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("test-1", "local test -------- " + i);
            producer.send(record, (data, ex) -> {
                long offset = data.offset();
                int partition = data.partition();
                String topic = data.topic();
//                System.out.println("topic = " + topic + ",offset = " + offset + ", partition = " + partition);
            });
        }
        Thread.sleep(2000);
    }
}
