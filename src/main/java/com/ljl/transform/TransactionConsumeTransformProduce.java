package com.ljl.transform;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

public class TransactionConsumeTransformProduce {
    private static final String brokerList = "bigdata01:9092,bigdata02:9092,bigdata03:9092";

    private static Properties getConsumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "study");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return properties;
    }

    private static Properties getProducerProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "709409");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    public static void main(String[] args) {
        // 初始化生产者和消费者
        KafkaProducer<String, String> producer = new KafkaProducer<>(getProducerProperties());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getConsumerProperties());

        // 消费者订阅主题
        consumer.subscribe(Collections.singleton("first"));

        // 生产者初始化事务
        producer.initTransactions();

        // 消费者拉取消息
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            if (!records.isEmpty()) {
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                // 生产者开启事务
                producer.beginTransaction();
                try {
                    for (TopicPartition partition : records.partitions()) {
                        List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                        for (ConsumerRecord<String, String> record : partitionRecords) {
                            ProducerRecord<String, String> producerRecord  = new ProducerRecord<>("second", record.key(), record.value());
                            producer.send(producerRecord, new Callback() {
                                @Override
                                public void onCompletion(RecordMetadata metadata, Exception exception) {
                                    if (exception == null) {
                                        System.out.println(metadata.topic() + "-" + metadata.partition() + "-" + metadata.offset());
                                    } else {
                                        exception.printStackTrace();
                                    }
                                }
                            });
                            long lastConsumerOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                            offsets.put(partition, new OffsetAndMetadata(lastConsumerOffset + 1));
                        }
                        // 提交消费位移
                        producer.sendOffsetsToTransaction(offsets, "study");
                        // 提交事务
                        producer.commitTransaction();
                    }
                } catch (ProducerFencedException e) {
                    producer.abortTransaction();
                    e.printStackTrace();
                }
            }
        }
    }
}
