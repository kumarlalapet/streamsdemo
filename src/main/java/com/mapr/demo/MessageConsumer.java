package com.mapr.demo;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
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
        //arg[3] - fileName

        FileWriter fw = new FileWriter(args[3], true);
        BufferedWriter bw = new BufferedWriter(fw);
        PrintWriter writer = new PrintWriter(bw);

        topic = args[0];

        configureConsumer(args);

        // Subscribe to the topic.
        List<String> topics = new ArrayList();
        topics.add(topic);
        consumer.subscribe(topics, new RebalanceListener());

        // Set the timeout interval for requests for unread messages.
        long pollTimeOut = 3000;

        try {
            do {
                // Request unread messages from the topic.
                ConsumerRecords<String, String> consumerRecords = consumer.poll(pollTimeOut);

                Iterator<ConsumerRecord<String, String>> iterator = consumerRecords.iterator();
                if (iterator.hasNext()) {
                    while (iterator.hasNext()) {
                        ConsumerRecord<String, String> record = iterator.next();

                        if (processRecords(record)) {
                            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                            offsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()+1));
                            consumer.commitAsync(offsets, new OffsetCommitCallback() {
                                @Override
                                public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                                    //Any after commit work can be done here
                                }
                            });

                            String message = record.toString();
                            String value = record.value();
                            System.out.println((" Consumed Record: " + message));
                            writer.println(value);
                            writer.flush();

                        }

                    }
                }

            } while (true);
        } finally {
            consumer.close();
            writer.close();
        }
    }

    private static boolean processRecords(ConsumerRecord<String, String> record) {
        return true;
    }

    static class RebalanceListener implements ConsumerRebalanceListener {

        public RebalanceListener() {
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            for(TopicPartition partition: partitions) {
                offsets.put(partition, new OffsetAndMetadata(consumer.position(partition)) );
            }
            consumer.commitSync(offsets);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            for(TopicPartition partition: partitions) {
                consumer.seek(partition, consumer.position(partition));
            }
        }
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
