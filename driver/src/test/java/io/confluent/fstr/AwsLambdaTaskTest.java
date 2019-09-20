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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


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

    String payload1 = jsonPayloadConverter.convertRecords(records);
    InvokeRequest request = new InvokeRequest()
            .withFunctionName("serverless-simple-http-endpoint-beta-recordsHandler")
            .withInvocationType(InvocationType.RequestResponse).withPayload(payload1);


    InvokeResult invoke = faas.invoke(request);
    ByteBuffer payload = invoke.getPayload();
    System.out.println("resuts:" + new String(payload.array()));
  }


  @Test
  public void simpleLambdaTest() {

    String sendPayload = "[\n" +
            "    {\n" +
            "        \"payload\": {\n" +
            "            \"timestamp\": 1562844607000,\n" +
            "            \"topic\": \"mytopic\",\n" +
            "            \"partition\": 1,\n" +
            "            \"offset\": 43822,\n" +
            "            \"key\": \"key1\",\n" +
            "            \"value\": \"value1\"\n" +
            "        }\n" +
            "    },\n" +
            "    {\n" +
            "        \"payload\": {\n" +
            "            \"timestamp\": 1562844608000,\n" +
            "            \"topic\": \"mytopic\",\n" +
            "            \"partition\": 1,\n" +
            "            \"offset\": 43823,\n" +
            "            \"key\": \"key2\",\n" +
            "            \"value\": \"value2\"\n" +
            "        }\n" +
            "    }\n" +
            "]";

    InvokeRequest request = new InvokeRequest()
            .withFunctionName("serverless-simple-http-endpoint-beta-connectorHandler")
            .withInvocationType(InvocationType.RequestResponse).withPayload(sendPayload);

        AWSLambdaAsync faas = getFaaS();
        InvokeResult invoke = faas.invoke(request);
        ByteBuffer payload = invoke.getPayload();
        System.out.println(">:" + invoke.getStatusCode()  +" Payload: " + new String(payload.array()));

  }

  @Test
  public void scaleLambdaTest() {


//    AWSLambdaAsync faas = getFaaS();

    InvokeRequest request = new InvokeRequest()
//      .withFunctionName("serverless-simple-http-endpoint-beta-recordsHandler")
            .withFunctionName("serverless-simple-http-endpoint-beta-neilHandler")
      .withInvocationType(InvocationType.RequestResponse).withPayload("{ \"stuff\": 100 }");

    long start = System.currentTimeMillis();

//    InvokeResult coldInvocation = faas.invoke(request);


    int totalMessages = 10000;
    long middle = System.currentTimeMillis();
    ExecutorService executorService = Executors.newFixedThreadPool(100);
    final AtomicInteger total = new AtomicInteger();
    final AtomicLong totalElapsed = new AtomicLong();

    for (int i = 0; i< totalMessages; i++) {

      final int cc = i;
      executorService.submit(() -> {

        AWSLambdaAsync faas = getFaaS();
        long a1 = System.currentTimeMillis();
        InvokeResult invoke = faas.invoke(request);
        ByteBuffer payload = invoke.getPayload();
        long a2 = System.currentTimeMillis();
        System.out.println(cc + "-" + total.incrementAndGet() + ">:" + invoke.getStatusCode() + " latency:" + (a2-a1)  +" Payload: " + new String(payload.array()));
        totalElapsed.addAndGet(a2-a1);

      });

   }
    try {
      executorService.shutdown();
      executorService.awaitTermination(20, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }


    long end = System.currentTimeMillis();
//    long hotElapsed = end - middle;
    double seconds = (double)(end - start)/1000.0;
    double ratePerSecond = (double)totalMessages/seconds;
    double avgHotElaped = (double)totalElapsed.get()/(double)totalMessages;

    System.out.println("Elapsed:" + seconds + " Rate-EventsPerSecond:" + ratePerSecond);
    System.out.println("avgHotElapsedMs:" + avgHotElaped + " cold:" + (middle - start));
  }

  private AWSLambdaAsync getFaaS() {
    AWSCredentialsProvider credentialsProvider = DefaultAWSCredentialsProviderChain.getInstance();

    return AWSLambdaAsyncClientBuilder.standard()
      .withRegion("eu-west-2")
      .withCredentials(credentialsProvider)
      .build();
  }

}
