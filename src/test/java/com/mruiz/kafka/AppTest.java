package com.mruiz.kafka;

import com.mruiz.kafka.constants.IKafkaConstants;
import com.mruiz.kafka.consumer.ConsumerCreator;
import com.mruiz.kafka.producer.ProducerCreator;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Unit test for simple App.
 */
public class AppTest extends TestCase {
  /**
   * Create the test case
   *
   * @param testName name of the test case
   */
  public AppTest(String testName) {
    super(testName);
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite() {
    return new TestSuite(AppTest.class);
  }

  /**
   * Rigourous Test :-)
   */
  public void testApp() {
    assertTrue(true);
  }

  static void runProducer(int numMessages) {
    Producer<Long, String> producer = ProducerCreator.createProducer();

    for (int index = 0; index < numMessages; index++) {
      final ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(IKafkaConstants.TOPIC_NAME,
          "This is record " + index);
      try {
        RecordMetadata metadata = producer.send(record).get();
      } catch (ExecutionException e) {
        System.out.println("Error in sending record");
        System.out.println(e);
      } catch (InterruptedException e) {
        System.out.println("Error in sending record");
        System.out.println(e);
      }
    }
  }

  static void runConsumer(){
    Consumer<Long, String> consumer = ConsumerCreator.createConsumer();
    int noMessageToFetch = 0;

    while (true) {
      final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
      if (consumerRecords.count() == 0) {
        noMessageToFetch++;
        if (noMessageToFetch > 0)
          break;
        else
          continue;
      }

      consumerRecords.forEach(record -> {
        System.out.println("Record value " + record.value());
      });
      consumer.commitAsync();
    }
    consumer.close();
  }

  /**
   * 1000 mensajes de texto
   *
   */
  public void test1000Messages1() {
    final int N_MESSAGES = 1000;
    runProducer(N_MESSAGES);
    runConsumer();
  }

  /**
   * 1000 mensajes de texto
   * 1 cola
   * 2 partitions
   */
  public void test1000Messages2(){
    final int N_MESSAGES = 1000;
    AdminClient adminClient = KafkaAdminClient.create(buildDefaultClientConfig());
    runProducer(N_MESSAGES);
    runConsumer();
  }
  public void test1000Messages3(){}

  /**
   * Creates a topic in Kafka. If the topic already exists this does nothing.
   * @param topicName - the namespace name to create.
   * @param partitions - the number of partitions to create.
   */
  public void createTopic(final String topicName, final int partitions) {
    final short replicationFactor = 1;

    // Create admin client
    try (final AdminClient adminClient = KafkaAdminClient.create(buildDefaultClientConfig())) {
      try {
        // Define topic
        final NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);

        // Create topic, which is async call.
        final CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singleton(newTopic));

        // Since the call is Async, Lets wait for it to complete.
        createTopicsResult.values().get(topicName).get();
      } catch (InterruptedException | ExecutionException e) {
        if (!(e.getCause() instanceof TopicExistsException)) {
          throw new RuntimeException(e.getMessage(), e);
        }
        // TopicExistsException - Swallow this exception, just means the topic already exists.
      }
    }
  }

  /**
   * Internal helper method to build a default configuration.
   */
  private Map<String, Object> buildDefaultClientConfig() {
    Map<String, Object> defaultClientConfig = Maps.newHashMap();
    defaultClientConfig.put("bootstrap.servers", getKafkaConnectString());
    defaultClientConfig.put("client.id", "test-consumer-id");
    return defaultClientConfig;
  }
}
