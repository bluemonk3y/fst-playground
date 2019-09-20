package io.confluent.fstr;

import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventBuilder;
import io.confluent.fstr.model.Payment;
import io.confluent.fstr.util.JsonSerializer;
import io.confluent.fstr.utils.IntegrationTestHarness;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

public class KafkaTestGeneration {


  private IntegrationTestHarness testHarness;

  private static final String INPUT_TOPIC = "lambda-test-input";
  private static final String RESPONSE_TOPIC = "lambda-test-response";
  private static final String ERROR_TOPIC = "lambda-test-error";


  @Before
  public void before() throws Exception {
    testHarness = new IntegrationTestHarness();
    testHarness.start("localhost:9092");

    System.setProperty("bootstrap.servers", testHarness.bootstrapServers());

  }

  @After
  public void after() {
    testHarness.stop();
  }


  @Test
  public void generateAWSLambdaProducerData() throws Exception {


    Map<String, CloudEvent> records = new HashMap<>();


    Payment data = new Payment("id", "11", "neil", "john", new BigDecimal(10), Payment.State.incoming, System.currentTimeMillis());
    CloudEvent<Payment> payment = getPaymentCloudEvent(data, data.getId());

    records.put(payment.getData().get().getId(), payment);

    CloudEvent<Payment> payment2 = getPaymentCloudEvent(new Payment("id", "12", "neil", "john", new BigDecimal(10), Payment.State.incoming, System.currentTimeMillis()), "12");

    records.put(payment2.getData().get().getId(), payment2);



    testHarness.produceData(INPUT_TOPIC, records, new JsonSerializer<>(), System.currentTimeMillis());


    /**
     * Expecting response payloads
     * [
     *     {
     *         "payload": {
     *             "timestamp": 1562844607000,
     *             "topic": "mytopic",
     *             "partition": 1,
     *             "offset": 43822,
     *             "result": .....
     *         }
     *     },
     *     {
     *         "payload": {
     *             "timestamp": 1562844608000,
     *             "topic": "mytopic",
     *             "partition": 1,
     *             "offset": 43823,
     *             "result": .....
     *         }
     *     }
     *     ....
     * ]
     */
//    Map<String, String> stringStringMap = testHarness.consumeData(RESPONSE_TOPIC, 3, new StringDeserializer(), new StringDeserializer(), 10000);
//
//    System.out.println(stringStringMap);

  }

  private CloudEvent<Payment> getPaymentCloudEvent(Payment data, String id) throws URISyntaxException, UnknownHostException {
    return new CloudEventBuilder<Payment>()
            .type(data.getClass().getTypeName())
            .id(id)
            .source(new URI("http://" + InetAddress.getLocalHost().getHostAddress()))
            .data(data)
            .build();
  }


//  private String asJson(String topic, int a, int b) {
//    Struct struct = new Struct(INPUT_SCHEMA)
//            .put("a", a)
//            .put("b", b);
//
//    byte[] raw = converterWithSchemaEnabled.fromConnectData(topic, INPUT_SCHEMA, struct);
//    return new String(raw, StandardCharsets.UTF_8);
//  }
//  private Properties properties() {
//    return null;
//  }

}
