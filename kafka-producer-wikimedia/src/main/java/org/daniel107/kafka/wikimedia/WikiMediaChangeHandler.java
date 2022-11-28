package org.daniel107.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikiMediaChangeHandler implements EventHandler {
    private KafkaProducer<String, String> kafkaProducer;
    private String topic;
    private static final Logger logger = LoggerFactory.getLogger(WikiMediaChangeHandler.class);

    public WikiMediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    @Override
    public void onOpen() throws Exception {
        // Nothing to do once stream is open
    }

    @Override
    public void onClosed() throws Exception {
        // Closing reading from stream
        // Need to close producer
        this.kafkaProducer.flush();
        this.kafkaProducer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {
        // Async code to publish to the topic
        logger.info(messageEvent.getData());
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, messageEvent.getData());
        this.kafkaProducer.send(producerRecord);
    }

    @Override
    public void onComment(String comment) throws Exception {
        // Nothing to do
    }

    @Override
    public void onError(Throwable t) {
        logger.error("Error in stream reading", t);
    }
}
