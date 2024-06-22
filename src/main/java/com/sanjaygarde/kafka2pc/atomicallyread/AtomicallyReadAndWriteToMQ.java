package com.sanjaygarde.kafka2pc.atomicallyread;

import com.ecom.supplychain.PurchaseOrder;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.KafkaException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

public class AtomicallyReadAndWriteToMQ {
        public static void main(String[] args) { //throws IOException, TimeoutException, URISyntaxException, NoSuchAlgorithmException, KeyManagementException {

            final String topic = "orders";

            Properties props = new Properties();
            // Add additional properties.
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "MQCrossWriter");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.CLIENT_ID_CONFIG, "MQCrossWriter-1");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer");
            props.put(io.confluent.kafka.serializers.KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, PurchaseOrder.class.getName());

            //for handling transactions
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

            ConnectionFactory factory = new ConnectionFactory();
            try {
                factory.setUri("amqp://localhost");  // Replace with your RabbitMQ server URI
            } catch (URISyntaxException | KeyManagementException | NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }

            try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
                channel.queueDeclare("orders", true, false, false, null);

                try (final Consumer<String, JsonNode> consumer = new KafkaConsumer<>(props)) {
                    consumer.subscribe(Collections.singletonList(topic));
                    while (true) {
                        ConsumerRecords<String, JsonNode> records = consumer.poll(Duration.ofMillis(100));

                        for (ConsumerRecord<String, JsonNode> record : records) {
                            String key = record.key();

                            ObjectMapper objMapper = new ObjectMapper();
                            PurchaseOrder po = objMapper.convertValue(record.value(), new TypeReference<PurchaseOrder>() {});

                            String jsonMessage = objMapper.writeValueAsString(po);
                            System.out.println("Json string:" + objMapper.writeValueAsString(po));

                            AMQP.Tx.SelectOk selectOK = channel.txSelect();
                            System.out.println( "Is select ok? " + selectOK );

                            System.out.println("Press any key to continue to publish to queue");

                            // Use a Scanner to capture a single key press
                            Scanner scanner = new Scanner(System.in);
                            scanner.nextLine(); // Wait for user input
                            System.out.println("publishing to the queue...");

                            channel.basicPublish("", "orders", null, jsonMessage.getBytes(StandardCharsets.UTF_8));
                            channel.txCommit();

                            System.out.println("published, press any key to continue to commit consumer group offsets");

                            // Use a Scanner to capture a single key press
                            scanner.nextLine(); // Wait for user input
                            System.out.println("committing offsets...");

                            consumer.commitSync();

                            System.out.println("done !" + jsonMessage);
                        }
                    }
                }catch(KafkaException ke){
                    ke.printStackTrace();
                    System.out.println("Rolling back message published to queue . . .");
                    channel.txRollback(); // This does not work as expected, we may need compensating transactions.
                    System.out.println(" done !");
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                    throw new RuntimeException(ioe);
                }
            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
}
