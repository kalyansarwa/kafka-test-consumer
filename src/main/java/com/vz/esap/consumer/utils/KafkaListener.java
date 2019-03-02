package com.vz.esap.consumer.utils;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class KafkaListener {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaListener.class);

	@Value("${kafka.zookeper.servers}")
	private String zookeperServers;

	@Value("${kafka.bootstrap.servers}")
	private String bootstrapServers;

	@Value("${kafka.group.id}")
	private String groupId;

	@Value("${kafka.enable.auto.commit}")
	private boolean enableAutoCommit;

	@Value("${kafka.fetch.min.bytes}")
	private int fetchMinBytes;

	@Value("${kafka.receive.buffer.bytes}")
	private int receiveBufferBytes;

	@Value("${kafka.max.partition.fetch.bytes}")
	private int maxPartitionFetchBytes;

	@Value("${kafka.serializer}")
	private String kafkaSerializer;

	@Value("${kafka.deserializer}")
	private String kafkaDeserializer;

	@Value("${kafka.topic}")
	private String kafkaTestTopic;

	@Value("${kafka.ssl.enabled.protocols}")
	private String sslEnabledProtocols;

	private KafkaListenThread consumerThread = null;
	private KafkaConsumer<String, String> consumer = null;

	public void start() {
		LOGGER.info("Starting KafkaListener");

		initializeKafkaConsumer();

		// start our custom thread that will listen and process kafka events
		consumerThread = new KafkaListenThread(null);
		consumerThread.alive = true;
		Thread timerThread = new Thread(consumerThread);
		timerThread.setName("KafkaListenerTimerThread");
		timerThread.start();
	}

	public void stop() {
		LOGGER.info("Stopping KafkaListener");
		consumerThread.alive = false;
		// don't call consumer.close() here. It happens in the run() function
	}

	private void initializeKafkaConsumer() {
		Properties kafkaConfigProps = readAndPopulateKafkaProperties();

		consumer = new KafkaConsumer<String, String>(kafkaConfigProps);

		LOGGER.info("Registering as a kafka subscriber to topic: {}", kafkaTestTopic);

		consumer.subscribe(Arrays.asList(kafkaTestTopic));
	}

	public Properties readAndPopulateKafkaProperties() {

		Properties kafkaProps = new Properties();

		kafkaProps.put("bootstrap.servers", bootstrapServers);
		kafkaProps.put("group.id", groupId);
		kafkaProps.put("enable.auto.commit", enableAutoCommit);
		kafkaProps.put("fetch.min.bytes", fetchMinBytes);
		kafkaProps.put("receive.buffer.bytes", receiveBufferBytes);
		kafkaProps.put("max.partition.fetch.bytes", maxPartitionFetchBytes);

		kafkaProps.put("key.deserializer", kafkaDeserializer);
		kafkaProps.put("value.deserializer", kafkaDeserializer);

		kafkaProps.put("ssl.enabled.protocols", sslEnabledProtocols);
		
		kafkaProps.put("heartbeat.interval.ms", "30000");
		kafkaProps.put("session.timeout.ms", "90000");
		kafkaProps.put("request.timeout.ms", "120000");

		SortedMap<String, Object> sortedkafkaProps = new TreeMap(kafkaProps);
		Set<String> keySet = sortedkafkaProps.keySet();
		Iterator<String> iterator = keySet.iterator();

		StringBuilder sb = new StringBuilder();
		sb.append("\n{");
		while (iterator.hasNext()) {
			String propName = iterator.next();
			sb.append("\n    " + propName + " = " + kafkaProps.get(propName) + ",");
		}
		sb.append("\n}");

		LOGGER.info("populatedKafkaProperties : " + sb.toString());

		return kafkaProps;
	}

	public void kafkaRecordProcess(ConsumerRecord<String, String> record) {
		LOGGER.info("[KAFKA_LISTNER_TOPIC_KAFKATEST] RECIEVED MESSAGE FROM THE TOPIC [" + record.topic()
				+ "] PROCESSING :::::: " + record.value());

		try {
			String receivedMessage = record.value();
			MessageProcessorThread createInventoryThread = new MessageProcessorThread(receivedMessage);
			Thread timerThread = new Thread(createInventoryThread);
			timerThread.setName("MessageProcessorThread");
			timerThread.start();

		} catch (Exception e) {
			LOGGER.error("KafkaListenThread Exception " + e);
		}
	}

	class KafkaListenThread implements Runnable {
		private boolean alive = true;

		public KafkaListenThread(Object q) {
		}

		@Override
		public void run() {
			long millis = 0L;

			while (alive) {
				millis = 1000L * 10;

				LOGGER.trace("calling KafkaConsumer.poll()");

				ConsumerRecords<String, String> records = consumer.poll(millis);

				if (records != null) {
					String zookeeperConnect = zookeperServers;
					try {
						int numReplicas = zookeeperConnect.split(",").length;
						LOGGER.trace(" numReplicas ::: " + numReplicas);
						for (ConsumerRecord<String, String> record : records) {
							kafkaRecordProcess(record);
						}
					} finally {
						LOGGER.trace("inside finally block");
					}
				}
			}
			// shutdown the consumer
			consumer.close();
			LOGGER.info("Stopped KafkaListener");
		}
	}

	class MessageProcessorThread implements Runnable {
		private String thisMessage;

		public MessageProcessorThread(String message) {
			thisMessage = message;
		}

		@Override
		public void run() {
			try {
				// Invoke a service class method that would process the received message here
				LOGGER.info(
						"Kafka Consumer service has received this message: " + thisMessage + ", will process it now.");
			} catch (Exception e) {
				LOGGER.info("Stopped KafkaListener", e);
			}
		}
	}

}
