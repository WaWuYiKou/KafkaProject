package com.ljl.offsetcustomstorage;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

public class KConsumer {
    private static Properties properties = null;
    private static KafkaConsumer<String, String> consumer = null;
    private static String group = "mysql_offset";
    static {
        properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.brokerList);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumer = new KafkaConsumer<String, String>(properties);
    }

    public static void main(String[] args) {
        consumer.subscribe(Collections.singletonList(Constants.topic), new ConsumerRebalanceListener() {
            // rebalance之前调用
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                for (TopicPartition partition : partitions) {
                    int topic_partition_id = partition.partition();
                    long topic_partition_offset = consumer.position(partition);
                    String date = new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(
                            new Date(new Long(System.currentTimeMillis()))
                    );
                    // TODO: 2021/9/21 将offset信息存入MySQL中
                    DBUtils.update("replace into offset values(?,?,?,?,?)",
                            new Offset(group, Constants.topic, topic_partition_id, topic_partition_offset, date));
                }
            }
            // rebalance之后读取之前的消费记录，继续消费
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                for (TopicPartition partition : partitions) {
                    int topic_partition_id = partition.partition();
                    // TODO: 2021/9/21 根据消费者组，主题，分区ID，查出offset值
                    long offset = DBUtils.queryOffset(
                            "select topic_partition_offset from offset where consumer_group = ? and topic = ? and topic_partition_id = ?",
                            group,
                            Constants.topic,
                            topic_partition_id
                    );
                    System.out.println("partition = " + topic_partition_id + ", offset = " + offset);
                    // 定位到最近提交的offset位置继续消费
                    consumer.seek(partition, offset);
                }
            }
        });
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            List<Offset> offsets = new ArrayList<>();
            for (ConsumerRecord<String, String> record : records) {
                String date = new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(
                        new Date(new Long(System.currentTimeMillis()))
                );
                offsets.add(new Offset(group, Constants.topic, record.partition(), record.offset(), date));
                System.out.println("|---------------------------------------------------------------\n" +
                        "|group\ttopic\tpartition\toffset\ttimestamp\n" +
                        "|" + group + "\t" + Constants.topic + "\t" + record.partition() + "\t" + record.offset() + "\t" + record.timestamp() + "\n" +
                        "|---------------------------------------------------------------"
                );
            }
            for (Offset offset : offsets) {
                // TODO: 2021/9/21 将offset信息存入MySQL中
                DBUtils.update("replace into offset values(?,?,?,?,?)", offset);
            }
            offsets.clear();
        }
    }
}
