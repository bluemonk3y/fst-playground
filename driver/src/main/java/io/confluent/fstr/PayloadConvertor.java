package io.confluent.fstr;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;
import java.util.HashMap;

public class PayloadConvertor {
    private ObjectMapper objectMapper = new ObjectMapper();


    public String convertRecords(Collection<SinkRecord> records) throws JsonProcessingException {
        SinkRecordPayload sinkRecordPayload = new SinkRecordPayload(records);
        return new String(this.objectMapper.writeValueAsBytes(sinkRecordPayload));

    }

    public String convert(SinkRecord record) throws JsonProcessingException {
        Object value = record.value();
        Schema schema = record.valueSchema();
        String topic = record.topic();

        JsonConverter jsonConverter = new JsonConverter();
        HashMap<String, Object> configs = new HashMap<>();
        configs.put("schemas.enable", false);
        jsonConverter.configure(configs, false);
        byte[] a = jsonConverter.fromConnectData(topic, schema, value);
        JsonDeserializer jsonDeserializer = new JsonDeserializer();
        jsonDeserializer.configure(configs, false);
        JsonNode b = jsonDeserializer.deserialize(record.topic(), a);

        String payload = objectMapper.writeValueAsString(b);

        //log.trace("P: {}", payload);

        return payload;
    }
}
