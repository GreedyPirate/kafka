package com.ttyc.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Properties;

public class ProducerTest {

    public static void main(String[] args) throws InterruptedException {
        sendNoStop("foo");
    }

    public static void send (String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("linger.ms", 100);
        props.put("acks", "all");


        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        List<PartitionInfo> partitionInfos = producer.partitionsFor(topic);
        partitionInfos.forEach(t->{
            System.out.println(t);
        });
        for (int i = 0; i < 10000; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, 1, "key" + i,  "local test -------- " + i);
            producer.send(record, (data, ex) -> {
                long offset = data.offset();
                int partition = data.partition();
                System.out.println("send to partition " + partition);
            });
        }
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
        }
    }

    public static void sendNoStop(String topic) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("linger.ms", 100);
        props.put("acks", "all");


        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        List<PartitionInfo> partitionInfos = producer.partitionsFor(topic);
        partitionInfos.forEach(t->{
            System.out.println(t);
        });
        for (;;) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, 1, "key",  "local test -------- ");
            producer.send(record, (data, ex) -> {
                long offset = data.offset();
                int partition = data.partition();
                System.out.println("send to partition " + partition);
            });
        }
    }
}
