package com.mapr.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.*;

/**
 * Created by mlalapet on 5/5/16.
 */
public class MessageConsumer {
    // Set the stream and topic to read from.
    public static String topic = "/streams/eventstream:log_topic_ag02";

    // Declare a new consumer.
    public static KafkaConsumer consumer;

    public static void main(String[] args) throws IOException {

        //arg[0] - topic
        //arg[1] - group id
        //arg[2] - client id

        topic = args[0];

        configureConsumer(args);

        // Subscribe to the topic.
        List<String> topics = new ArrayList();
        topics.add(topic);
        consumer.subscribe(topics);

        // Set the timeout interval for requests for unread messages.
        long pollTimeOut = 1000;

        try {
            do {
                // Request unread messages from the topic.
                ConsumerRecords<String, String> consumerRecords = consumer.poll(pollTimeOut);

                /**
                 if (processRecords(consumerRecords)) {
                 consumer.commitSync();
                 }
                 **/

                Iterator<ConsumerRecord<String, String>> iterator = consumerRecords.iterator();
                if (iterator.hasNext()) {
                    while (iterator.hasNext()) {
                        ConsumerRecord<String, String> record = iterator.next();

                        if (processRecords(record)) {
                            System.out.println((" Consumed Record: " + record.toString()));

                            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                            offsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()));
                            consumer.commitSync(offsets);
                        }

                    }
                }

            } while (true);
        } finally {
            consumer.close();
        }
    }

    private static boolean processRecords(ConsumerRecords<String, String> consumerRecords) {
        Iterator<ConsumerRecord<String, String>> iterator = consumerRecords.iterator();
        if (iterator.hasNext()) {
            while (iterator.hasNext()) {
                ConsumerRecord<String, String> record = iterator.next();
                System.out.println((" Consumed Record: " + record.toString()));
            }
        }

        return true;
    }

    private static boolean processRecords(ConsumerRecord<String, String> record) {
        //Done processing record
        return true;
    }

    /* Set the value for a configuration parameter.
       This configuration parameter specifies which class
       to use to deserialize the value of each message.*/
    public static void configureConsumer(String[] args) {
        Properties props = new Properties();
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        props.put("group.id", args[1]);
        props.put("client.id", args[2]);
        props.put("enable.auto.commit", "false");
        consumer = new KafkaConsumer<String, String>(props);
    }

}
