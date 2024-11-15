package com.sanjaygarde.kafka2pc.client.atomicallyread;

import com.ecom.supplychain.PurchaseOrder;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.sanjaygarde.kafka2pc.CommonUtilityTask;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeoutException;

public class AtomicallyReadAndWriteToMQ {
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    private static final String KAFKA_CONSUMER_GROUP_ID = "MQ_CROSS_WRITER";
    private static final String KAFKA_CLIENT_ID = "MQ_CROSS_WRITER_1";
    private static final String ORDERS_TOPIC = "orders";

    private static final String AMQP_HOST = "amqp://localhost";
    private static final String ORDERS_QUEUE = "orders";

    private static final AtomicallyReadAndWriteToMQ synchWriter = new AtomicallyReadAndWriteToMQ();

    private void readAndWriteInSynch() {
        Map<TopicPartition, Long> lastPolledOffsets = new HashMap<>();
        Scanner scanner = new Scanner(System.in);

        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setUri(AMQP_HOST);  // Replace with your RabbitMQ server URI

            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            channel.queueDeclare(ORDERS_QUEUE, true, false, false, null);

            try (final Consumer<String, JsonNode> kafkaConsumer =
                         new CommonUtilityTask().createKafkaConsumer( KAFKA_BOOTSTRAP_SERVERS,
                                 SCHEMA_REGISTRY_URL,
                                 KAFKA_CONSUMER_GROUP_ID,
                                 KAFKA_CLIENT_ID,
                                 false )){
                         //createKafkaConsumer( KAFKA_BOOTSTRAP_SERVERS, KAFKA_CONSUMER_GROUP_ID, KAFKA_CLIENT_ID )) {
                kafkaConsumer.subscribe(Collections.singletonList( ORDERS_TOPIC ));

                while (true) {
                    ConsumerRecords<String, JsonNode> records = kafkaConsumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, JsonNode> record : records) {


                        TopicPartition topicPartition = new  TopicPartition( record.topic(), record.partition() );
                        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata( record.offset() );
                        HashMap<TopicPartition, OffsetAndMetadata> partitionAndMetadata = new HashMap<>();
                        partitionAndMetadata.put(topicPartition, offsetAndMetadata);

                        ObjectMapper objMapper = new ObjectMapper();
                        PurchaseOrder po = objMapper.convertValue(record.value(), new TypeReference<PurchaseOrder>() {});

                        System.out.println("Read message with key: " + po.getOrderId() + " from kafka topic: " + ORDERS_TOPIC);

                        String jsonMessage = objMapper.writeValueAsString(po);
                        //System.out.println("Json string:" + objMapper.writeValueAsString(po));

                        channel.txSelect();

                        System.out.print("Press any key to publish to the message queue: " + ORDERS_QUEUE + "..." );

                        // Use a Scanner to capture a single key press
                        scanner.nextLine(); // Wait for user input

                        try { //Enclose only transaction statements in this block, keep it minimal
                            channel.basicPublish("", "orders", null, jsonMessage.getBytes(StandardCharsets.UTF_8));

                            System.out.println("done.");
                            System.out.print("Press any key to commit kafkaConsumer group offsets");
                            // Use a Scanner to capture a single key press
                            scanner.nextLine(); // Wait for user input
                            kafkaConsumer.commitSync( partitionAndMetadata );

                            System.out.println("done");
                            System.out.print("Press any key to commit message to the queue . . . ");
                            scanner.nextLine(); // Wait for user input
                            channel.txCommit();
                            System.out.println("done.");

                        }catch(KafkaException ke){
                            ke.printStackTrace();
                            System.out.println("==============================================");
                            System.out.print("Rolling back message published to queue . . .");
                            channel.txRollback(); // This does not work as expected, we may need compensating transactions.
                            System.out.println(" done !");
                            System.out.println("==============================================");

                            break;//start over, poll kafka again

                        }catch (IOException | AlreadyClosedException ioe) {
                            ioe.printStackTrace();
                            System.out.println("==============================================");
                            System.out.print("Queue commit failed, resetting kafkaConsumer offsets . . . ");

                            kafkaConsumer.seek(topicPartition, record.offset() - 1 );

                            System.out.println("done.");
                            System.out.println("==============================================");

                            break;//start over, poll kafka again
                        }
                    }
                }
            }
        } catch (IOException | TimeoutException | URISyntaxException | NoSuchAlgorithmException | KeyManagementException | KafkaException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        synchWriter.readAndWriteInSynch();
    }
}
