package com.ljl.offsetcustomstorage;

public class Offset {
    private String consumer_group;
    private String topic;
    private Integer topic_partition_id;
    private Long topic_partition_offset;
    private String timestamp;

    public Offset() {
    }

    public Offset(String consumer_group, String topic, Integer topic_partition_id, Long topic_partition_offset, String timestamp) {
        this.consumer_group = consumer_group;
        this.topic = topic;
        this.topic_partition_id = topic_partition_id;
        this.topic_partition_offset = topic_partition_offset;
        this.timestamp = timestamp;
    }

    public String getConsumer_group() {
        return consumer_group;
    }

    public void setConsumer_group(String consumer_group) {
        this.consumer_group = consumer_group;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getTopic_partition_id() {
        return topic_partition_id;
    }

    public void setTopic_partition_id(Integer topic_partition_id) {
        this.topic_partition_id = topic_partition_id;
    }

    public Long getTopic_partition_offset() {
        return topic_partition_offset;
    }

    public void setTopic_partition_offset(Long topic_partition_offset) {
        this.topic_partition_offset = topic_partition_offset;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Offset{" +
                "consumer_group='" + consumer_group + '\'' +
                ", topic='" + topic + '\'' +
                ", topic_partition_id=" + topic_partition_id +
                ", topic_partition_offset=" + topic_partition_offset +
                ", timestamp='" + timestamp + '\'' +
                '}';
    }
}
