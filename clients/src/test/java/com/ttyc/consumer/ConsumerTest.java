package com.ttyc.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerTest {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.100.101.122:9092,10.100.97.35:9092,10.9.171.147:9092");
        props.put("group.id", "consumer-201-src");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("test-1"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5L));
            if(records.count() > 0) {
                for (ConsumerRecord<String, String> record : records){
                    System.out.println(record.key() + "-------" + record.value());
                }
            }

        }
    }
}
