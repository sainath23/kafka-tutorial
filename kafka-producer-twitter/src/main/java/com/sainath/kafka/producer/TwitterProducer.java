package com.sainath.kafka.producer;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(TwitterProducer.class);
    private static final String CONSUMER_KEY = "Pj6TmP7BDTAw1beRSnWq8HWt7";
    private static final String CONSUMER_SECRET = "kJN6Vxu13T2Fe1m7aEaxXFnJttSCzxDr77tZy6aHzTxgHO5pxI";
    private static final String TOKEN = "110342062-g7yS76UDwjQkNfPoed5vQFXAHsWymH3LNgQgOweB";
    private static final String SECRET = "mWOiOwqem6IWGFCh7hz4Z7QvqCyL19KQKv2kQe8pLhs1y";
    private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    private static final String TOPIC = "twitter_tweets";
    private static final List<String> TERMS = Lists.newArrayList("bitcoin", "usa", "cricket");

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        LOGGER.info("Setup");
        /* Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);

        // create twitter client
        Client twitterClient = createTwitterClient(msgQueue);

        // Attempt to establish a connection
        twitterClient.connect();

        // create kafka producer
        KafkaProducer<String, String> kafkaProducer = createKafkaProducer();

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(
                () -> {
                    LOGGER.info("Stopping application...");
                    LOGGER.info("Stopping twitter client...");
                    twitterClient.stop();
                    LOGGER.info("Flushing kafka producer...");
                    kafkaProducer.flush();
                    LOGGER.info("Closing kafka producer...");
                    kafkaProducer.close();
                    LOGGER.info("Done!");
                }
        ));

        // loop to send tweets to kafka
        // on a different thread, or multiple different threads....
        while (!twitterClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                twitterClient.stop();
            }

            if (msg != null) {
                LOGGER.info(msg);
                kafkaProducer.send(new ProducerRecord<>(TOPIC, null, msg), (recordMetadata, e) -> {
                    // Lambda to implement onCompletion method
                    // Executes every time when a record is successfully sent or an exception is thrown
                    if (e == null) {
                        // The record was successfully sent
                        LOGGER.info("Received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        LOGGER.error("Error while producing", e);
                    }
                });
            }
        }
        LOGGER.info("End of application");
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        return new KafkaProducer<>(getKafkaProducerProperties());
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        /* Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts twitterHost = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        hosebirdEndpoint.trackTerms(TERMS);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, SECRET);

        ClientBuilder builder = new ClientBuilder()
                .name("Doitgeek-Client-01")
                .hosts(twitterHost)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }

    private Properties getKafkaProducerProperties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create safer producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // high throughput producer (at the expense of bit of latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); // 32KB batch size

        return properties;
    }
}
