package com.sanjaygarde.kafka2pc.client.atomicallywrite;

import com.ecom.supplychain.PurchaseOrder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.sanjaygarde.kafka2pc.CommonUtilityTask;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class AtomicallyWriteToKafkaAndMQ {
    private static final String kafkaBootstrapServers = "localhost:9092";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    private static final String kafkaClientID= "MQCrossReader24";
    private static final String ordersTopic = "orders";
    private static final String uncommittedOrdersTopic = "uncommitted_orders";

    private static final String recordFile = "orders.txt";

    private static final ArrayList<PurchaseOrder> poList = new ArrayList<>();

    private static final AtomicallyWriteToKafkaAndMQ synchWriter = new AtomicallyWriteToKafkaAndMQ();

    /**
     * This method is for demo only. You cannot load all the records at once and crash the application because of the limited memory.
     * In reality you would read it from some microservice or socket or another system like a database or message queue.
     */
    private void loadRecordsFromAnExternalSource(){
        try (BufferedReader reader = new BufferedReader(new FileReader(recordFile))) {
            String lineRead = null;
            ObjectMapper objMapper = new ObjectMapper();

            while ( (lineRead = reader.readLine()) != null ){
                PurchaseOrder po = objMapper.readValue(lineRead, PurchaseOrder.class);
                poList.add(po);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeInSync() {
        KafkaProducer<String, PurchaseOrder> producer = //createKafkaProducer();
                new CommonUtilityTask().createKafkaProducer(kafkaBootstrapServers, SCHEMA_REGISTRY_URL, kafkaClientID);

        ConnectionFactory factory = new ConnectionFactory();
        try {
            factory.setUri("amqp://localhost");  // Replace with your RabbitMQ server URI
        } catch (URISyntaxException | KeyManagementException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            channel.queueDeclare("orders", true, false, false, null);

            for (PurchaseOrder po : poList) {

                AMQP.Tx.SelectOk selectOK = channel.txSelect();
                System.out.println("Is select ok? " + selectOK);

                System.out.println("Press any key to  publish to queue");

                // Use a Scanner to capture a single key press
                Scanner scanner = new Scanner(System.in);
                scanner.nextLine(); // Wait for user input
                System.out.println("publishing to the queue...");

                ObjectMapper objMapper = new ObjectMapper();
                String jsonMessage = objMapper.writeValueAsString(po);
                System.out.println("Json string:" + objMapper.writeValueAsString(po));
                System.out.println("done");

                channel.basicPublish("", "orders", null, jsonMessage.getBytes(StandardCharsets.UTF_8));

                try {
                    // Use a Scanner to capture a single key press
                    scanner.nextLine(); // Wait for user input
                    System.out.println("publishing to the topic...");
                    ProducerRecord<String, PurchaseOrder> record = new ProducerRecord<String, PurchaseOrder>(ordersTopic, po.getOrderId(), po);
                    producer.send(record).get();//yes, a blocking call. Choose your battles, do you want high performance or data integrity
                    System.out.println("done");

                    System.out.println("Committing the record: " + po.getOrderId() + " to the queue.");
                    channel.txCommit();
                    System.out.println("done");
                } catch (InterruptedException | ExecutionException | KafkaException e) {
                    e.printStackTrace();
                    System.out.println("Rolling back record sent to the queue:" + po.getOrderId() + " ..... ");
                    channel.txRollback();
                    System.out.println("done");
                }catch (IOException e) {
                    e.printStackTrace();
                    System.out.println("Committing record: " + po.getOrderId() + " to the queue failed, publishing record to " + uncommittedOrdersTopic);
                    ProducerRecord<String, PurchaseOrder> record = new ProducerRecord<String, PurchaseOrder>(uncommittedOrdersTopic, po.getOrderId(), po);
                    try {
                        producer.send(record).get();
                    } catch (InterruptedException | ExecutionException ex) {
                        System.out.println("********************* Note: ******************************");
                        System.out.println("Publishing record: " + po.getOrderId() + " to kafka topic: " + uncommittedOrdersTopic + " also failed !");
                        System.out.println("***********************************************************");
                    }
                    System.out.println("done");
                }
            }
        }catch (IOException | TimeoutException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        synchWriter.loadRecordsFromAnExternalSource();
        synchWriter.writeInSync();
    }
}
