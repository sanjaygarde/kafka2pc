package com.sanjaygarde.kafka2pc.client.atomicallywrite;

import com.ecom.supplychain.PurchaseOrder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjaygarde.kafka2pc.CommonUtilityTask;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

public class AtomicallyWriteToKafkaAndDB {
    private static final String kafkaBootstrapServers = "localhost:9092";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    private static final String kafkaClientID= "MQCrossReader24";
    private static final String ordersTopic = "orders";
    private static final String uncommittedOrdersTopic = "uncommitted_orders";

    private static final String connectionUrl = "jdbc:mysql://localhost:6603/bankdb";
    private static final String SQL_INSERT = "INSERT INTO Orders ( order_id, purchaser_id, item_id, price, quantity, order_date) VALUES (?,?,?,?,?,?)";

    private static final String recordFile = "orders.txt";

    private static final ArrayList<PurchaseOrder> poList = new ArrayList<>();

    private static final AtomicallyWriteToKafkaAndDB synchWriter = new AtomicallyWriteToKafkaAndDB();


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

        Connection conn = null;
        try {
            conn = DriverManager.getConnection(connectionUrl, "root", "my-secret-pw");
            PreparedStatement preparedStatement = conn.prepareStatement(SQL_INSERT);
            conn.setAutoCommit(false);

            for (PurchaseOrder po : poList) {
                //we first write to DB so that it can be rolled back as there is no roll back option in Kafka
                preparedStatement.setString(1, po.getOrderId());
                preparedStatement.setString(2, po.getPurchaserId());
                preparedStatement.setString(3, po.getItemId());
                preparedStatement.setDouble(4, po.getPrice());
                preparedStatement.setInt(5, po.getQuantity());
                preparedStatement.setDate(6, po.getOrderDate());
                //for handling transactions
                preparedStatement.executeUpdate();

                System.out.println("Press any key to publish: " + po.getOrderId() + " to Kafka topic");

                // Use a Scanner to capture a single key press
                Scanner scanner = new Scanner(System.in);
                scanner.nextLine(); // Wait for user input
                System.out.println("publishing to the topic...");

                try {
                    ProducerRecord<String, PurchaseOrder> record = new ProducerRecord<String, PurchaseOrder>(ordersTopic, po.getOrderId(), po);
                    producer.send(record).get();//yes, a blocking call. Choose your battles, do you want high performance or data integrity
                    System.out.println("done");

                    System.out.println("Committing the record: " + po.getOrderId() + " to the table.");
                    conn.commit();
                    System.out.println("done");
                } catch (InterruptedException | ExecutionException | KafkaException e) {
                    e.printStackTrace();
                    System.out.println("Rolling back database record:" + po.getOrderId() + " ..... ");
                    conn.rollback();
                    System.out.println("done");
                }catch(SQLException e){
                    e.printStackTrace();
                    System.out.println("Committing record: " + po.getOrderId() + " to database failed, publishing record to " + uncommittedOrdersTopic);
                    ProducerRecord<String, PurchaseOrder> record = new ProducerRecord<String, PurchaseOrder>(uncommittedOrdersTopic, po.getOrderId(), po);
                    try {
                        producer.send(record).get();
                    } catch (InterruptedException | ExecutionException ex) {
                        System.out.println("********************* Note: ******************************");
                        System.out.println("Publishing record: " + po.getOrderId() + " to kafka topic: " + uncommittedOrdersTopic + " also failed !");
                        System.out.println("***************************************************");
                    }
                    System.out.println("done");
                }
            }
        } catch(SQLException e){
            e.printStackTrace();
        }finally {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
            producer.close();
        }
    }

    public static void main(String[] args) {
        synchWriter.loadRecordsFromAnExternalSource();
        synchWriter.writeInSync();
    }
}
