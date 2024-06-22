package com.sanjaygarde.kafka2pc.atomicallyread;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Collections;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

import com.ecom.supplychain.PurchaseOrder;

public class TestQueueConsumer {

    public static Connection connection = null;
    public static Channel channel = null;

    public static void main(String[] args) {
        ConnectionFactory factory = new ConnectionFactory();

        try {
            factory.setUri("amqp://localhost");  // Replace with your RabbitMQ server URI
        } catch (URISyntaxException | KeyManagementException | NoSuchAlgorithmException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        try {
            connection = factory.newConnection();
            channel =  connection.createChannel();
            final AMQP.Queue.DeclareOk orders = channel.queueDeclare("orders", true, false, false, null);
//            channel.basicConsume("orders",
//                    false,
//                    TestQueueConsumer::handle,
//                    consumerTag -> {} );
//
//            channel.basicAck(envelope.getDeliveryTag(), false);

            // Create the consumer
            DefaultConsumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    String message = new String(body, "UTF-8");
                    System.out.println("Received message: '" + message + "'");

                    // Acknowledge the message
                    channel.basicAck(envelope.getDeliveryTag(), false);
                }
            };

            // Start consuming messages
            channel.basicConsume("orders", false, consumer);

        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    static void handle(String consumerTag, Delivery delivery) throws UnsupportedEncodingException {
        String valueRead = new String(delivery.getBody(), "UTF-8").toString();
        System.out.println("Printing here: " + valueRead);
        ObjectMapper objMapper = new ObjectMapper();
        PurchaseOrder po = null;

        try {
            po = objMapper.readValue(valueRead, PurchaseOrder.class);
            System.out.println("OrderID: " + po.getOrderId());
            channel.txCommit();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
