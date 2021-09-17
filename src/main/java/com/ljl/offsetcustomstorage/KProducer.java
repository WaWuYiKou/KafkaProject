package com.ljl.offsetcustomstorage;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KProducer {

    private static Properties properties = null;
    private static KafkaProducer<String, String> producer = null;
    static {
        properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.brokerList);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "709409");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(properties);
    }

    public Properties getProperties() {
        return properties;
    }

    public KafkaProducer<String, String> getProducer() {
        return producer;
    }
}
