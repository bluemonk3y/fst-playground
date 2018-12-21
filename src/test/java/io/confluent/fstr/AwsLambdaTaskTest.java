package io.confluent.fstr;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.AWSLambdaAsyncClientBuilder;
import com.amazonaws.services.lambda.model.InvocationType;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.*;


import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.junit.Assert.assertEquals;

public class AwsLambdaTaskTest {

  private static final String TOPIC = "aws-lambda-topic";
  private static final int PARTITION = 12;
  private static final int PARTITION2 = 13;

  private static final TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC, PARTITION);
  private static final TopicPartition TOPIC_PARTITION2 = new TopicPartition(TOPIC, PARTITION2);


  @Test
  public void connectRecord() throws JsonProcessingException {



    Schema valueSchema = SchemaBuilder.struct()
            .name("com.example.Person")
            .field("name", STRING_SCHEMA)
            .field("age", Schema.INT32_SCHEMA)
            .build();

    String bobbyMcGee = "Bobby McGee";
    int value21 = 21;

    Struct value = new Struct(valueSchema)
            .put("name", bobbyMcGee)
            .put("age", value21);


    Collection<SinkRecord> records = new ArrayList<>();
    SinkRecord sinkRecord = new SinkRecord(
            TOPIC,
            1,
            STRING_SCHEMA,
            "key",
            valueSchema,
            value,
            0,
            System.currentTimeMillis(),
            TimestampType.CREATE_TIME);
    records.add(sinkRecord);
    records.add(sinkRecord);
    records.add(sinkRecord);


    AWSLambdaAsync faas = getFaaS();

    PayloadConvertor jsonPayloadConverter = new PayloadConvertor();

    InvokeRequest request = new InvokeRequest()
            .withFunctionName("serverless-simple-http-endpoint-beta-recordsHandler")
            .withInvocationType(InvocationType.RequestResponse).withPayload(jsonPayloadConverter.convertRecords(records));


    InvokeResult invoke = faas.invoke(request);
    ByteBuffer payload = invoke.getPayload();
    System.out.println("resuts:" + new String(payload.array()));
  }

  @Test
  public void simpleLambdaTest() {


    AWSLambdaAsync faas = getFaaS();

    InvokeRequest request = new InvokeRequest()
      .withFunctionName("serverless-simple-http-endpoint-beta-recordsHandler")
      .withInvocationType(InvocationType.RequestResponse).withPayload("{ \"stuff\": 100 }");

    long start = System.currentTimeMillis();

    InvokeResult coldInvocation = faas.invoke(request);


    int totalMessages = 100;
    long middle = System.currentTimeMillis();
    for (int i = 0; i< totalMessages; i++) {
    InvokeResult invoke = faas.invoke(request);
    ByteBuffer payload = invoke.getPayload();
      System.out.println("resuts:" + new String(payload.array()));
    }
    long end = System.currentTimeMillis();
    long hotElapsed = end - middle;
    double seconds = (double)hotElapsed/1000.0;
    double ratePerSecond = (double)totalMessages/seconds;
    double avgHotElaped = (double)hotElapsed/(double)totalMessages;

    System.out.println("Elapsed Hot:" + seconds + " Total:" + (end - start));
    System.out.println("avgHotPerEventMs:" + avgHotElaped + " cold:" + (middle - start) + " hot EPS:" + ratePerSecond);
  }

  private AWSLambdaAsync getFaaS() {
    AWSCredentialsProvider credentialsProvider = DefaultAWSCredentialsProviderChain.getInstance();

    return AWSLambdaAsyncClientBuilder.standard()
      .withRegion("eu-west-2")
      .withCredentials(credentialsProvider)
      .build();
  }

}
