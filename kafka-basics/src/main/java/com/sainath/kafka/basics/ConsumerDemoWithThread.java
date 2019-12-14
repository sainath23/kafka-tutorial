package com.sainath.kafka.basics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    private static final String GROUP_ID = "my-sixth-application";
    private static final String TOPIC = "first_topic";

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    public void run() {
        final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Runnable runnable = new ConsumerThread(countDownLatch);
        Thread myThread = new Thread(runnable);
        myThread.start();

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerThread) runnable).shutdown();
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application exited!");
        }));

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerThread implements Runnable {
        private final Logger logger = LoggerFactory.getLogger(ConsumerThread.class);
        private CountDownLatch countDownLatch;
        private KafkaConsumer<String, String> kafkaConsumer;

        public ConsumerThread(CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;

            // create consumer config
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // create consumer
            this.kafkaConsumer = new KafkaConsumer<>(properties);

            // subscribe
            kafkaConsumer.subscribe(Collections.singleton(TOPIC));

        }

        @Override
        public void run() {
            // poll for new data
            try {
                while (true) {
                    ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : consumerRecords) {
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal");
            } finally {
                kafkaConsumer.close();
                // tell our main code we're done with consumer
                countDownLatch.countDown();
            }

        }

        public void shutdown() {
            // wakeup() method is a special method to interrupt kafkaConsumer.poll()
            kafkaConsumer.wakeup();
        }
    }
}
