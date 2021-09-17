package com.ljl.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.util.Properties;

public class TransactionalProducer {
    public static void main(String[] args) {
        // 1.创建配置信息
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "bigdata01:9092,bigdata02:9092,bigdata03:9092");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "709409");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 2.创建生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        producer.initTransactions();
        producer.beginTransaction();

        try {
            // 3.生产数据
            for (int i = 0; i < 10; i++) {
                producer.send(new ProducerRecord<>("first", "i = " + i),
                        new Callback() {
                            @Override
                            public void onCompletion(RecordMetadata metadata, Exception exception) {
                                if (exception == null) {
                                    System.out.println(metadata.topic() + "-" + metadata.partition() + "-" + metadata.offset());
                                } else {
                                    exception.printStackTrace();
                                }
                            }
                        });
            }

            // 4.提交事务
            producer.commitTransaction();
        } catch (ProducerFencedException e) {
            e.printStackTrace();
            // 5.撤回事务
            producer.abortTransaction();
        }

        // 6.关闭资源
        producer.close();
    }
}
