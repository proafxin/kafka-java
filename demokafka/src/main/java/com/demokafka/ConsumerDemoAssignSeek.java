package com.demokafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("KAFKA_BOOTSTRAP_SERVER"));
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        String topicName = "first_topic";

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {

            TopicPartition partitionReadFrom = new TopicPartition(topicName, 1);
            long offsetReadFrom = 15L;

            consumer.assign(Arrays.asList(partitionReadFrom));

            consumer.seek(partitionReadFrom, offsetReadFrom);

            final int NUM_MESSAGES_TO_READ = 5;
            boolean keepReading = true;
            int numMessagesRead = 0;

            while (keepReading) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Key: " + record.key());
                    logger.info("Offset: " + record.offset());
                    logger.info("Partition: " + record.partition());
                    logger.info("Value: " + record.value());

                    numMessagesRead++;

                    if (numMessagesRead >= NUM_MESSAGES_TO_READ) {
                        keepReading = false;
                        break;
                    }
                }
            }

            logger.info("Application exiting");
        }

    }
}
