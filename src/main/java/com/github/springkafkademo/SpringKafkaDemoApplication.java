package com.github.springkafkademo;

import com.github.springkafkademo.engine.Consumer;
import com.github.springkafkademo.engine.Producer;
import io.netty.util.Timeout;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class SpringKafkaDemoApplication {


	public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {

		SpringApplication.run(SpringKafkaDemoApplication.class, args);
		String topic = "spring-kafka-demo";

		Producer producer = new Producer(topic);
		Thread thread = new Thread(() -> {
			for(int i = 0; i < 100; i++) {
				try {
					producer.send(String.valueOf(i), "hello from producer");
				} catch (ExecutionException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				try {
					TimeUnit.SECONDS.sleep(5);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
		thread.start();

		Consumer consumer = new Consumer(topic);

		consumer.consume(record -> System.out.println("key = " + record.key() + " value = " + record.value()));

		TimeUnit.MINUTES.sleep(10);
		producer.close();
		consumer.close();
	}

}
