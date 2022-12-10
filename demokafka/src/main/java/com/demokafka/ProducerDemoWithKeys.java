package com.demokafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithKeys {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("KAFKA_BOOTSTRAP_SERVER"));
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 20; i++) {
            String message = "hello world" + Integer.toString(i);
            String key = "id_" + Integer.toString(i);
            String topicName = "first_topic";
            logger.info("Key set: " + key);
            final ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, message);
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Received new metadata. Topic: " + recordMetadata.topic());
                        logger.info(("Partition: " + recordMetadata.partition()));
                        logger.info("Offset: " + recordMetadata.offset());
                        logger.info(("Timestamp: " + recordMetadata.timestamp()));
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            });
        }

        producer.flush();
        producer.close();
    }
}
