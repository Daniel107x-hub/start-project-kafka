package org.daniel107x.kafka.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoCooperative {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoCooperative.class);

    public static void main(String[] args) {
        logger.info("I'm a kafka consumer!");

        // Create consumer properties
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-third-app";
        String topic = "demo_java";

        Properties properties =new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // none/earliest/latest
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName()); //Force cooperative assignment
//        properties.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "")

        // Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Get reference to current thread
        final Thread mainThread = Thread.currentThread();

        // Adding shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                logger.info("Detected a shutdown, lets exit by calling consumer.wakeup()...");
                consumer.wakeup(); // The next consumer.poll will throw a wakeUpException
                // Join the main thread to allow execution of code on main thread
                try {
                    mainThread.join();
                }catch(InterruptedException e){
                    e.printStackTrace();
                }
                super.run();
            }
        });

        try {
            // Suscribe consumer to topics
            consumer.subscribe(Arrays.asList(topic));

            // Poll new data
            while (true) {
                logger.info("Polling");
                /**
                 * Poll kafka and get as many records as possible.
                 * If there are no records, wait 100 milis just to check if we are able to get any records
                 * If no records are found, empty collection will be returned
                 */
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Key: {}, Value: {}, Offsets: {}, Partition: {}", record.key(), record.value(), record.offset(), record.partition());
                }
            }
        }catch(WakeupException e){
            logger.info("Wake up exception");
            // Ignore because this is an expected exception when closing a consumer
        }catch(Exception e){
            logger.error("Unexpected exception");
        }finally{
            consumer.close(); //This will also commit the offsets
            logger.info("The consumer is now gracefully closed");
        }
    }
}
