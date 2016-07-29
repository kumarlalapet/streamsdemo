package com.mapr.demo;

import com.google.gson.Gson;

import java.io.*;
import java.util.UUID;
import java.util.concurrent.*;

/**
 * Created by mlalapet on 7/28/16.
 */
public class RandomLogGenerator {

    private static final ExecutorService executor = Executors.newFixedThreadPool(2);

    static class JsonData {
        public String key;
        public String message;
    }

    static class MessageProducerThread implements Callable {
        private final String logFile;
        public boolean runVal = true;
        private long sleepInMs;
        //private KafkaProducer producer;
        PrintWriter writer;

        MessageProducerThread(String sleepInMs, String logFile) throws IOException {
            this.sleepInMs = Long.parseLong(sleepInMs);
            this.logFile = logFile;
            //configureProducer();
            FileWriter fw = new FileWriter(logFile, true);
            BufferedWriter bw = new BufferedWriter(fw);
            writer = new PrintWriter(bw);
        }

        @Override
        public Boolean call() {
            while (runVal) {
                JsonData data = new JsonData();
                data.key = UUID.randomUUID().toString();
                data.message = "Hello from " + data.key + "!";

                Gson gson = new Gson();
                final String message = gson.toJson(data);

                try {

                    writer.println(message);
                    writer.flush();

                } catch (Exception ex) {

                }

                try {
                    Thread.sleep(this.sleepInMs);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            try {
                writer.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (!runVal) {
                System.out.println("Shutting down the thread!");
                return Boolean.TRUE;
            } else
                return Boolean.FALSE;

        }
    }

    public static void main(String args[]) throws IOException, ExecutionException, InterruptedException {
        //args[0] - sleep in milli seconds
        //args[1] - filename to create

        MessageProducerThread task = new MessageProducerThread(args[0], args[1]);
        Future<Boolean> future = executor.submit(task);
        while (true) {
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            String input = br.readLine();

            if ("q".equals(input)) {
                System.out.println("Exit!");
                task.runVal = false;
                Boolean val = future.get();
                System.out.println("Future value " + val.booleanValue());
                if (val.booleanValue()) {
                    Thread.sleep(1000 * 10);
                    System.out.println("Cleaned up!");
                    System.exit(0);
                } else {
                    Thread.sleep(1000 * 10);
                    System.err.println("Something wrong happened while shutting down!");
                    System.exit(0);
                }
            }
        }

    }
}
