package com.github.springkafkademo.engine;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer implements Closeable {

    //private final KafkaConsumer consumer = getConsumer();
    private KafkaConsumer consumer;
    private String topic;

    public Consumer(String topic) {
        this.topic = topic;
        this.consumer = getConsumer();
    }

    private KafkaConsumer<String, String> getConsumer() {
        final Properties consumerConfigs = new Properties();
        this.topic = topic;
        consumerConfigs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, "groupId");
        consumerConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerConfigs);
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }

    public void consume(java.util.function.Consumer<ConsumerRecord<String, String>> recordConsumer) {
        Thread thread = new Thread(() -> {
            while (true) {
                ConsumerRecords records = consumer.poll(Duration.ofSeconds(1));
                records.forEach(record -> recordConsumer.accept((ConsumerRecord<String, String>) record));
            }
        });
        thread.start();
    }

    @Override
    public void close() throws IOException {
        consumer.close();
    }
}
