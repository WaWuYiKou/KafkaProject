package com.ljl.offsetcustomstorage;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;
import java.util.Random;

public class KProducer {
    private static Properties properties = null;
    private static KafkaProducer<String, String> producer = null;

    static {
        properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.brokerList);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "709409");
        properties.put(ProducerConfig.RETRIES_CONFIG, "3");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(properties);
    }

    public static void main(String[] args) {
        producer.initTransactions();
        producer.beginTransaction();
        for (int i = 0; i < 100; i++) {
            System.out.println("第" + (i + 1) + "条消息开始发送");
            sendData();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        producer.commitTransaction();
        producer.close();
    }

    private static String generateHash(String topic) {
        String hashStr = "";
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            int nextInt = new Random().nextInt(1000);
            digest.update((topic + nextInt).getBytes());
            byte[] bytes = digest.digest();
            BigInteger bigInteger = new BigInteger(1, bytes);
            String str = bigInteger.toString(16);
            hashStr = str.substring(0, 3) + topic + nextInt;
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return hashStr;
    }

    private static void sendData() {
        producer.send(new ProducerRecord<>(
                Constants.topic,
                generateHash(Constants.topic),
                new Random().nextInt(1000) + "\t金锁家庭财产综合险(家顺险)\t1\t金锁家庭财产综合险(家顺险)\t213\t自住型家财险\t10\t家财保险\t44\t人保财险\t23:50.0"
        ), new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    String topic = metadata.topic();
                    int partition = metadata.partition();
                    long offset = metadata.offset();
                    System.out.println(topic + "-" + partition + "-" + offset);
                } else {
                    exception.printStackTrace();
                }
            }
        });
    }
}
