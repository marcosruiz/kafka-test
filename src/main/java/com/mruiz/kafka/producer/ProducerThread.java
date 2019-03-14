package com.mruiz.kafka.producer;

import com.mruiz.kafka.constants.IKafkaConstants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.util.concurrent.ExecutionException;

public class ProducerThread implements Runnable {


  int messagesToSend = 0;
  String fileName = "test_1.pdf";

  public ProducerThread() {
  }

  public ProducerThread(int messagesToSend) {
    this.messagesToSend = messagesToSend;
  }

  public ProducerThread(int messagesToSend, String fileName) {
    this.messagesToSend = messagesToSend;
    this.fileName = fileName;
  }

  @Override
  public void run() {
    Producer<Long, String> producer = ProducerCreator.createProducer();

    for (int index = 0; index < messagesToSend; index++) {
      ProducerRecord<Long, String> record;
      record = new ProducerRecord<Long, String>(IKafkaConstants.TOPIC_NAME, "This is record " + index);
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
}
