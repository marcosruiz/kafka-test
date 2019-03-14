package com.mruiz.kafka;

import com.mruiz.kafka.consumer.ConsumerThread;
import com.mruiz.kafka.producer.ProducerThread;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.util.ArrayList;

/**
 * Unit test for checking the performance of Kafka
 * Before you start these tests you need to:
 * start Zookeeper (I suppose you are in $KAFKA_HOME)
 * $ bin/zookeeper-server-start.sh config/zookeeper.properties
 * start Kafka
 * $ bin/kafka-server-start.sh config/server.properties
 * create a queue of 10 partitions which topic is 'demo'
 * $ bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 10 --topic demo
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

  /**
   * 1000 mensajes de texto
   */
  public void test1000Messages1Consumer() throws InterruptedException {
    final int N_MESSAGES = 1000;
    Thread tProducer = new Thread(new ProducerThread(N_MESSAGES));
    Thread tConsumer = new Thread(new ConsumerThread(0));
    tProducer.start();
    tConsumer.start();
    tProducer.join();
    tConsumer.join();
    assertTrue(true);
  }

  /**
   * 1000 mensajes de texto
   * 1 cola
   * 2 partitions
   */
  public void test1000Messages2Consumers() throws InterruptedException {
    final int N_MESSAGES = 1000;
    final int N_CONSUMERS = 2;
    // Producer
    Thread tProducer = new Thread(new ProducerThread(N_MESSAGES));
    tProducer.start();

    // Consumers
    ArrayList<Thread> arrayOfTConsumers = new ArrayList();
    for (int i = 0; i < N_CONSUMERS; i++) {
      arrayOfTConsumers.add(new Thread(new ConsumerThread(i)));
    }
    for (int i = 0; i < N_CONSUMERS; i++) {
      arrayOfTConsumers.get(i).start();
    }

    // Check everything is done
    tProducer.join();
    while (!arrayOfTConsumers.isEmpty()) {
      arrayOfTConsumers.remove(0).join();
    }
    assertTrue(true);
  }

  /**
   * 1000 mensajes de texto
   * 1 cola
   * 10 particiones
   */
  public void test1000Messages10Consumers() throws InterruptedException {
    final int N_MESSAGES = 1000;
    final int N_CONSUMERS = 10;
    // Producer
    Thread tProducer = new Thread(new ProducerThread(N_MESSAGES));
    tProducer.start();

    // Consumers
    ArrayList<Thread> arrayOfTConsumers = new ArrayList();
    for (int i = 0; i < N_CONSUMERS; i++) {
      arrayOfTConsumers.add(new Thread(new ConsumerThread(i)));
    }
    for (int i = 0; i < N_CONSUMERS; i++) {
      arrayOfTConsumers.get(i).start();
    }

    // Check everything is done
    tProducer.join();
    while (!arrayOfTConsumers.isEmpty()) {
      arrayOfTConsumers.remove(0).join();
    }
    assertTrue(true);
  }

  /**
   * 100K mensajes de texto
   * 1 cola
   * 10 particiones
   */
  public void test100KMessages10Consumers() throws InterruptedException {
    final int N_MESSAGES = 100000;
    final int N_CONSUMERS = 10;
    // Producer
    Thread tProducer = new Thread(new ProducerThread(N_MESSAGES));
    tProducer.start();

    // Consumers
    ArrayList<Thread> arrayOfTConsumers = new ArrayList();
    for (int i = 0; i < N_CONSUMERS; i++) {
      arrayOfTConsumers.add(new Thread(new ConsumerThread(i)));
    }
    for (int i = 0; i < N_CONSUMERS; i++) {
      arrayOfTConsumers.get(i).start();
    }

    // Check everything is done
    tProducer.join();
    while (!arrayOfTConsumers.isEmpty()) {
      arrayOfTConsumers.remove(0).join();
    }
    assertTrue(true);
  }

  /**
   *
   */

}
