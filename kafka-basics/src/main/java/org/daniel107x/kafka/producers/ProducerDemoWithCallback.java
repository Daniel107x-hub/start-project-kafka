package org.daniel107x.kafka.producers;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("I'm a kafka producer");

        // Create producer properties
        Properties properties =new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Send data - async operation (Non blocking)
        for(int i=0;i<10;i++){
            // Create a producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "Hello world: " + i);

            producer.send(producerRecord, new Callback() {
                @Override // Executed on record successfully sent or exception happens
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception == null){
                        // Record successfully sent
                        logger.info("Received new metadata: \n" +
                                "Topic: {} \n" +
                                "Partition: {} \n" +
                                "Offsets: {}\n" +
                                "Timestamp: {}", metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
                    }else{
                        logger.error("Error while producing: {}", exception);
                    }
                }
            });

            try{
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // Flush and close producer (Flush y a synchronous operation)(until all data is actually sent)
        producer.flush();
        producer.close();
    }
}
