package com.mapr.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by mlalapet on 5/5/16.
 */
public class MessageProducer {
    public static String topic = "/map/streams:cattopic";
    public static KafkaProducer producer;

    public static void main(String args[]) throws ExecutionException, InterruptedException {
        configureProducer(args);

        topic = args[0];

        //generating 1000 messages
        int count = 0;
        Future<RecordMetadata> response;
        do {
            String clientKey = getRandomClientKey();
            String message = "Message for customer("+(count+1)+") record "+clientKey;

            ProducerRecord rec = new ProducerRecord(topic, clientKey, message);
            response = producer.send(rec);
            producer.flush();

            RecordMetadata metaData = response.get();

            System.out.println(clientKey+" placed in partition "+metaData.partition()+" offset "+metaData.offset());

            count++;
        } while(count < 6001 && response.isDone());

        System.out.println("All Done.");
        producer.close();
    }

    public static void configureProducer(String[] args) {
        Properties props = new Properties();
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(props);
    }

    public static String getRandomClientKey() {
        int number[] = {1,2,3,4,5,6,7,8,9,0};
        String clientKey = "";
        for(int i =0; i<number.length; i++) {
            Random randomizer = new Random();
            clientKey = clientKey+ number[randomizer.nextInt(number.length)];
        }
        return clientKey;
    }
}
