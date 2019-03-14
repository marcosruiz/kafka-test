package com.mruiz.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import javax.xml.crypto.Data;
import java.io.*;

public class ConsumerThread implements Runnable {


  int id = 0;

  public ConsumerThread(int id) {
    this.id = id;
  }

  @Override
  public void run() {
    Consumer<Long, String> consumer = ConsumerCreator.createConsumer();
    int noMessageToFetch = 0;

    while (true) {
      final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
      if (consumerRecords.count() == 0) {
        noMessageToFetch++;
        if (noMessageToFetch > 0) {
          break;
        } else {
          continue;
        }
      }

      consumerRecords.forEach(record -> {
        System.out.println("Value of " + id + ": " + record.value());
      });
      consumer.commitAsync();
    }
    consumer.close();
  }
}
