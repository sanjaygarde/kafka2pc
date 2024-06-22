package com.sanjaygarde.kafka2pc.atomicallywrite;

import com.ecom.supplychain.PurchaseOrder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;


public class AtomicallyReadFromMQAndWriteToKafka {
    private static final AtomicallyReadFromMQAndWriteToKafka syncReaderWriter = new AtomicallyReadFromMQAndWriteToKafka();
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";

    private static final String AMQP_HOST = "amqp://localhost";

    private static final String KAFKA_CLIENT_ID = "MQCrossReader24";

    private static final String ORDERS_TOPIC = "orders";
    private static final String ORDERS_QUEUE = "orders";

    private static final List<String> unacknowledgedMessages = new ArrayList<>();

    private KafkaProducer<String, PurchaseOrder> createKafkaProducer( ){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, KAFKA_CLIENT_ID);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer");

        return new KafkaProducer<>(props);
    }


    private void readAndWriteInSynch(){
        System.out.println("Consuming from message queue: " + ORDERS_QUEUE);
        ConnectionFactory factory = new ConnectionFactory();

        try {
            factory.setUri(AMQP_HOST);  // Replace with your RabbitMQ server URI

            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            channel.queueDeclare(ORDERS_QUEUE, true, false, false, null);

            // Create the consumer
            DefaultConsumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    String message = new String(body, StandardCharsets.UTF_8);
                    System.out.println("Received message from the queue: '" + message + "'");

                    ObjectMapper objMapper = new ObjectMapper();
                    PurchaseOrder poq = null;

                    try {
                        poq = objMapper.readValue(message, PurchaseOrder.class);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    System.out.println("------->" + poq.getOrderDate());

                    Scanner scanner = new Scanner(System.in);

                    if (unacknowledgedMessages.contains(poq.getOrderId())) {

                        System.out.println( poq.getOrderId() + " message already sent to Kafka, press any key to send ack to the queue " + ORDERS_QUEUE);
                        // Use a Scanner to capture a single key press
                        scanner.nextLine(); // Wait for user input

                        try {
                            channel.basicAck(envelope.getDeliveryTag(), false);
                            unacknowledgedMessages.remove(poq.getOrderId());

                            System.out.println( "ack sent.");
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    } else {
                        try (KafkaProducer<String, PurchaseOrder> producer = createKafkaProducer()) {
                            ProducerRecord<String, PurchaseOrder> record = new ProducerRecord<String, PurchaseOrder>(ORDERS_TOPIC, poq.getOrderId(), poq);

                            System.out.println(" Press any key to send " +  poq.getOrderId() + " to Kafka topic " + ORDERS_TOPIC);
                            scanner.nextLine(); // Wait for user input

                            PurchaseOrder finalPoq = poq;
                            Future<RecordMetadata> future = producer.send(record, (metadata, exception) -> {
                                if (exception == null) {
                                    System.out.printf("Sent record with key %s to partition %d with offset %d to Kafka",
                                            record.key(), metadata.partition(), metadata.offset());

                                    System.out.println("Press any key to send acknowledgment to the queue . . .");
                                    scanner.nextLine(); // Wait for user input
                                    try {
                                        channel.basicAck(envelope.getDeliveryTag(), false);
                                        System.out.println("done.");
                                    } catch (IOException e) {
                                        System.out.println("Message " + finalPoq.getOrderId() + " could not be ack\'ed to the queue, adding to unack\'ed internal cache.");
                                        unacknowledgedMessages.add(finalPoq.getOrderId());
                                    }
                                } else {
                                    // simply don't send an ack to the queue
                                    System.out.println("Message " + finalPoq.getOrderId() + " could not be sent to Kafka");
                                    System.out.println("Acknowledegment for the message " + finalPoq.getOrderId() + " will not be sent.");
                                    exception.printStackTrace();
                                }
                            });
                        }catch(KafkaException e){
                            e.printStackTrace();
                        }
                    }
                }
            };

            // Start consuming messages
            channel.basicConsume(ORDERS_QUEUE, false, consumer);

        }catch(IOException | TimeoutException | URISyntaxException | NoSuchAlgorithmException | KeyManagementException e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        syncReaderWriter.readAndWriteInSynch();
    }

}