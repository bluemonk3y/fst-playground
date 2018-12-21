package io.confluent.fstr;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayList;
import java.util.Collection;

public class SinkRecordPayload {

    private Schema keySchema;
    private Schema valueSchema;
    private String topic;


    private Integer kafkaPartition;
    private String timeStampType;
    private Collection<KeyValue> records = new ArrayList<KeyValue>();



    public SinkRecordPayload(){};
    public SinkRecordPayload(Collection<SinkRecord> records) throws JsonProcessingException {


        SinkRecord first = records.iterator().next();
        this.keySchema = first.keySchema();
        this.valueSchema = first.valueSchema();
        this.topic = first.topic();
        this.kafkaPartition = first.kafkaPartition();
        this.timeStampType = first.timestampType().name;
        PayloadConvertor payloadConvertor = new PayloadConvertor();
        for (SinkRecord record : records) {
            this.records.add(new KeyValue(record.key(), payloadConvertor.convert(record), record.timestamp(), record.kafkaOffset()));
        }
    }



    public Schema getKeySchema() {
        return keySchema;
    }

    public void setKeySchema(Schema keySchema) {
        this.keySchema = keySchema;
    }

    public Schema getValueSchema() {
        return valueSchema;
    }

    public void setValueSchema(Schema valueSchema) {
        this.valueSchema = valueSchema;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getKafkaPartition() {
        return kafkaPartition;
    }

    public void setKafkaPartition(Integer kafkaPartition) {
        this.kafkaPartition = kafkaPartition;
    }

    public String getTimeStampType() {
        return timeStampType;
    }

    public void setTimeStampType(String timeStampType) {
        this.timeStampType = timeStampType;
    }

    public Collection<KeyValue> getRecords() {
        return records;
    }

    public void setRecords(Collection<KeyValue> records) {
        this.records = records;
    }


    public static class KeyValue {
        private Object key;
        private Object value;
        private long timestamp;
        private long kafkaOffset;

        public KeyValue(){};

        public KeyValue(Object key, Object value, long timestamp, long kafkaOffset) {
            this.key = key;
            this.value = value;
            this.timestamp = timestamp;
            this.kafkaOffset = kafkaOffset;
        }

        public Object getKey() {
            return key;
        }

        public void setKey(Object key) {
            this.key = key;
        }

        public Object getValue() {
            return value;
        }

        public void setValue(Object value) {
            this.value = value;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public long getKafkaOffset() {
            return kafkaOffset;
        }

        public void setKafkaOffset(long kafkaOffset) {
            this.kafkaOffset = kafkaOffset;
        }
    }
}
