package com.vz.esap.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.EnableAsync;

import com.vz.esap.consumer.utils.KafkaListener;

@EnableAsync
@SpringBootApplication
public class ConsumerApplication {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerApplication.class);

	public ConsumerApplication() {
		startup();
	}

	public static void main(String[] args) {
		
		ApplicationContext context = SpringApplication.run(ConsumerApplication.class, args);

		LOGGER.info("=> ConsumerApplication startup - subscribeToKafka() called");
		KafkaListener kafkaListener = context.getBean(KafkaListener.class);
		kafkaListener.start();
		LOGGER.info("=> ConsumerApplication startup - subscribeToKafka() subscribed");
	}

	public void startup() {
		LOGGER.info("Starting ConsumerApplication...");
	}
}
