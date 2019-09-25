/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package io.confluent.fstr;

import io.confluent.fstr.model.Payment;
import io.confluent.fstr.utils.IntegrationTestHarness;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.kstream.Grouped;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class FaasFanTest {

//  private JsonConverter converterWithSchemaEnabled;
//  private JsonConverter converterWithSchemaDisabled;


  private static final String TASK_TOPIC = "task-input";
  private static final String TASK_FANOUT = "task-fanout";
  private static final String TASK_REDUCE = "task-reduce";
  Properties streamsConfig = getProperties("localhost:9091");



  private static Schema INPUT_SCHEMA = SchemaBuilder.struct()
          .field("a", Schema.INT32_SCHEMA)
          .field("b", Schema.INT32_SCHEMA)
          .build();




  private TopologyTestDriver testDriver;
  private IntegrationTestHarness testHarness;

  @Before
  public void before() throws Exception {

    testHarness = new IntegrationTestHarness();
    //testHarness.start("localhost:9092");
    testHarness.start();
    System.setProperty("bootstrap.servers", testHarness.bootstrapServers());
  }

  @After
  public void after() {
    testHarness.stop();
  }


  @Test
  public void gate() throws Exception {


    StreamsBuilder builder = new StreamsBuilder();
    KStream<String, Payment> incoming = builder.stream(TASK_TOPIC);

    incoming.filter((key, value) -> value.isComplete()).to(TASK_FANOUT);


    Topology gateTopology = builder.build();



    System.out.println("Topology:" + gateTopology.describe());

    testDriver = new TopologyTestDriver(gateTopology, streamsConfig);

    // setup
    ConsumerRecordFactory<String, Payment> factory = new ConsumerRecordFactory<>(
            TASK_TOPIC,
            new StringSerializer(),
            new Payment.Serde().serializer()
    );

    // test
    Payment payment = new Payment("txnId", "payment-id-1234","from","to", new BigDecimal(123.0), Payment.State.complete, System.currentTimeMillis());
    testDriver.pipeInput(factory.create(TASK_TOPIC, payment.getId(), payment));
    testDriver.close();


    // verify
    ProducerRecord<String, String> taskRecords1 = getStringStringProducerRecord(TASK_FANOUT);
    System.out.println("fan-task:" + taskRecords1.key() + ": " +  taskRecords1.value());

    ProducerRecord<String, String> taskRecords2 = getStringStringProducerRecord(TASK_FANOUT);
    System.out.println("fan-task:" + taskRecords2.key() + ": " +  taskRecords2.value());
    Assert.assertNotNull(taskRecords1);
  }


  @Test
  public void fanOut() throws Exception {


    StreamsBuilder builder = new StreamsBuilder();
    KStream<String, Payment> incoming = builder.stream(TASK_TOPIC);

    /**
     * Fan out from 1 payment to many
     */

    incoming.flatMapValues(payment -> {
              ArrayList<String> fanTasks = new ArrayList<>();
              for (int i = 0; i < payment.getAmount().intValue(); i++) {
                fanTasks.add("fan-" + i);
              }
              return fanTasks;
            }
    ).to(TASK_FANOUT);

    Topology fanOutTopology = builder.build();

    System.out.println("Topology:" + fanOutTopology.describe());

    testDriver = new TopologyTestDriver(fanOutTopology, streamsConfig);

    // setup
    ConsumerRecordFactory<String, Payment> factory = new ConsumerRecordFactory<>(
            TASK_TOPIC,
            new StringSerializer(),
            new Payment.Serde().serializer()
    );

    // test
    Payment payment = new Payment("txnId", "payment-id-1234","from","to", new BigDecimal(123.0), Payment.State.complete, System.currentTimeMillis());
    testDriver.pipeInput(factory.create(TASK_TOPIC, payment.getId(), payment));
    testDriver.close();


    // verify
    ProducerRecord<String, String> taskRecords1 = getStringStringProducerRecord(TASK_FANOUT);
    System.out.println("fan-task:" + taskRecords1.key() + ": " +  taskRecords1.value());

    ProducerRecord<String, String> taskRecords2 = getStringStringProducerRecord(TASK_FANOUT);
    System.out.println("fan-task:" + taskRecords2.key() + ": " +  taskRecords2.value());
    Assert.assertNotNull(taskRecords1);
  }

  @Test
  public void fanIn() throws Exception {

    Topology fanInTopology = buildFanIn(TASK_FANOUT, TASK_REDUCE);

    System.out.println(fanInTopology.describe());

    testDriver = new TopologyTestDriver(fanInTopology, streamsConfig);

    // setup
    ConsumerRecordFactory<String, String> factory = new ConsumerRecordFactory<>(
            TASK_FANOUT,
            new StringSerializer(),
            new StringSerializer()
    );

    // test
    testDriver.pipeInput(factory.create(TASK_FANOUT, "1", "fan-1:100"));
    testDriver.pipeInput(factory.create(TASK_FANOUT, "1", "fan-1:200"));
    testDriver.pipeInput(factory.create(TASK_FANOUT, "1", "fan-1:300"));
    testDriver.pipeInput(factory.create(TASK_FANOUT, "1", "fan-1:400"));
    testDriver.pipeInput(factory.create(TASK_FANOUT, "1", "fan-1:500 finalTask"));

    testDriver.close();


    // verify
    for (int i = 0; i < 10; i++) {
      ProducerRecord<String, String> taskRecords1 = getStringStringProducerRecord(TASK_REDUCE);
      System.out.println(i + ") slide-reduce:" + taskRecords1.key() + ": " + taskRecords1.value());
    }
//    Assert.assertNotNull(taskRecords1);


  }

  private ProducerRecord<String, String> getStringStringProducerRecord(String taskReduce) {
    return testDriver.readOutput(
            taskReduce,
            new StringDeserializer(),
            new StringDeserializer()
    );
  }

  static Topology buildFanIn(String collectionFromTopic, String reduceTopic) {
    final StreamsBuilder builder = new StreamsBuilder();
    builder.stream(collectionFromTopic, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .windowedBy(SessionWindows.with(Duration.ofMinutes(1)))
            .reduce(new Reducer<String>() {
              //String result = "";
              @Override
              //    * @param value1 the first value for the aggregation
              //     * @param value2 the second value for the aggregation
              // Return the new aggregate value
              public String apply(String v1, String v2) {
                System.out.println(" --- Reducing:" + v1 + ":" + v2);
                return v1 + "-" + v2;
              }
            })
            .toStream()
            .map((key, value) -> new KeyValue<>(key.key() + "@" + key.window().start() + "->" + key.window().end(), value))
            .to(reduceTopic, Produced.with(Serdes.String(), Serdes.String()));

    return builder.build();
  }


  private Properties getProperties(String broker) {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "TEST-APP-ID");// + System.currentTimeMillis());
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Payment.Serde.class.getName());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 2000);
    return props;
  }

}
