package com.github.springkafkademo.engine;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer implements Closeable {

    private KafkaProducer producer;
    private String topic;

    public Producer(String topic) {
        this.topic = topic;
        this.producer = getProducer();
    }

    private KafkaProducer<String, String> getProducer() {
        final Properties producerConfigs = new Properties();
        this.topic = topic;
        producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        producerConfigs.put(ProducerConfig.CLIENT_ID_CONFIG, "clientId");
        producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        return new KafkaProducer(producerConfigs);
    }

    public void send(String key, String value) throws ExecutionException, InterruptedException {
        producer
                .send(new ProducerRecord(topic, key, value))
                .get();
    }

    @Override
    public void close() throws IOException {
        producer.close();
    }
}
