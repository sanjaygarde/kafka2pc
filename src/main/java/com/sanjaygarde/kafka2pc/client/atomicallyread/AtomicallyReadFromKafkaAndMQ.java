package com.sanjaygarde.kafka2pc.client.atomicallyread;

import com.ecom.supplychain.PurchaseOrder;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;
import com.sanjaygarde.kafka2pc.CommonUtilityTask;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

public class AtomicallyReadFromKafkaAndMQ {

    private static final AtomicallyReadFromKafkaAndMQ syncReader = new AtomicallyReadFromKafkaAndMQ();
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";

    private static final String AMQP_HOST = "amqp://localhost";
    private static final String KAFKA_CONSUMER_GROUP_ID = "MQ_X_READER";

    private static final String KAFKA_CLIENT_ID = "MQ_X_READER_1";

    private static final String ORDERS_TOPIC = "orders";
    private static final String ORDERS_QUEUE = "orders";
    private static Channel channel;
    private static final String COMMITTED_ORDERS_TOPIC = "committed_orders";
    private static final String UNCOMMITTED_ORDERS_TOPIC = "uncommitted_orders";

    private static final Hashtable<String, PurchaseOrder> kafkaMessages = new Hashtable<>();
    private static final Hashtable<String, POWithDeliveryTag> mqMessages = new Hashtable<>();

    private static  Hashtable<String, PurchaseOrder> uncommitedMessages = null;

    //this is no longer needed
    private static final Hashtable<TopicPartition, OffsetAndMetadata> ordersOffsets = new Hashtable<>();

    private static final ExecutorService executorService = Executors.newFixedThreadPool(3);;

    private class POWithDeliveryTag{
        private PurchaseOrder po;
        private long deliveryTag;

        public POWithDeliveryTag(PurchaseOrder po, long deliveryTag) {
            this.po = po;
            this.deliveryTag = deliveryTag;
        }

        public PurchaseOrder getPo() {
            return po;
        }

        public void setPo(PurchaseOrder po) {
            this.po = po;
        }

        public long getDeliveryTag() {
            return deliveryTag;
        }

        public void setDeliveryTag(long deliveryTag) {
            this.deliveryTag = deliveryTag;
        }
    }

    public void start() {
        executorService.submit(() -> consumeFromKafka(KAFKA_BOOTSTRAP_SERVERS, kafkaMessages));
        executorService.submit(() -> consumeFromQueue(AMQP_HOST, ORDERS_QUEUE));

        executorService.submit(this::compareAndTransfer);

        // Shutdown gracefully
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            executorService.shutdown();
            try {
                executorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
    }

    private void consumeFromKafka(String bootstrapServer, Hashtable<String, PurchaseOrder> poRecords) {
        Consumer<String, JsonNode> consumer = //createKafkaConsumer(KAFKA_BOOTSTRAP_SERVERS, KAFKA_CONSUMER_GROUP_ID, ORDERS_TOPIC);
                new CommonUtilityTask().createKafkaConsumer(KAFKA_BOOTSTRAP_SERVERS,
                                                        SCHEMA_REGISTRY_URL,
                                                        KAFKA_CONSUMER_GROUP_ID,
                                                        KAFKA_CLIENT_ID,
                                                        true);
        consumer.subscribe(Arrays.asList(ORDERS_TOPIC));

        while (true) {
            consumer.poll(Duration.ofMillis(100)).forEach(record -> {
                System.out.println("Reading from kafka topic: " + ORDERS_TOPIC);
                PurchaseOrder po = new ObjectMapper().convertValue(record.value(), new TypeReference<PurchaseOrder>() {
                });
                System.out.println("po: " + po.getOrderId());
                kafkaMessages.put(po.getOrderId(), po);
                System.out.println("kafkaMessages.size():" + kafkaMessages.size());
            });
        }
    }

    private void consumeFromQueue(String amqpHost, String ordersQueue){
        System.out.println("Consuming from message queue: " + ordersQueue);

        ConnectionFactory factory = new ConnectionFactory();
        try {
            factory.setUri( amqpHost );
            Connection connection = factory.newConnection();
            this.channel = connection.createChannel();
            final AMQP.Queue.DeclareOk orders = channel.queueDeclare(ordersQueue, true, false, false, null);

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

                    mqMessages.put(poq.getOrderId(), new POWithDeliveryTag(poq, envelope.getDeliveryTag()));
                }
            };

            // Start consuming messages
            channel.basicConsume(ORDERS_QUEUE, false, consumer);

        } catch (IOException | TimeoutException | URISyntaxException | NoSuchAlgorithmException | KeyManagementException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private void compareAndTransfer() {
        Scanner scanner = new Scanner(System.in);
        CommonUtilityTask task = new CommonUtilityTask();

        while(true) {
            try {
                TimeUnit.SECONDS.sleep(5); // Adjust the interval as needed
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            Enumeration<String> kafkaMessagesKeyEnum = kafkaMessages.keys();
            while(kafkaMessagesKeyEnum.hasMoreElements()) {
                String poID = kafkaMessagesKeyEnum.nextElement();
                PurchaseOrder po = kafkaMessages.get(poID);

                if (mqMessages.containsKey(poID)) {
                    POWithDeliveryTag poDT = mqMessages.get(poID);

                    //the lines BELOW in the try block should be all or none
                    try {
                        System.out.println("Match found for: " + poID );

                        System.out.print("Press any key to publish:" + poID + " to committed topic . . . ");
                        scanner.nextLine(); // Wait for user input
                        //publish( COMMITTED_ORDERS_TOPIC, poID, po );

                        task.publish(KAFKA_BOOTSTRAP_SERVERS,
                                SCHEMA_REGISTRY_URL,
                                KAFKA_CLIENT_ID,
                                COMMITTED_ORDERS_TOPIC,
                                poID,
                                po);

                        kafkaMessages.remove( poID );
                        System.out.println(" done.");

                        if( uncommitedMessages.containsKey( poID ) ) {
                            //send a tombstone message to the uncommittedOrdersTopic
                            System.out.print( "Press any key to publish tombstone message to uncommitted topic for:" + poID );
                            scanner.nextLine(); // Wait for user input
                            //publish(UNCOMMITTED_ORDERS_TOPIC, poID, null);

                            task.publish(KAFKA_BOOTSTRAP_SERVERS,
                                    SCHEMA_REGISTRY_URL,
                                    KAFKA_CLIENT_ID,
                                    UNCOMMITTED_ORDERS_TOPIC,
                                    poID,
                                    null);

                            uncommitedMessages.remove( poID );
                            System.out.println(" done.");
                        }

                        System.out.println( "Press any key to send an ack to the queue for:" + poID + " . . .");
                        scanner.nextLine(); // Wait for user input
                        channel.basicAck( poDT.getDeliveryTag(), false );
                        System.out.println(" done.");

                        mqMessages.remove( poID );

                    } catch ( IOException e ) {
                        //Maybe save the msg ids and later run a batch to send all these acks when the mq is up
                        mqMessages.remove( poID );
                        System.out.println( "Ack could not be sent to the Message Queue for: " + poID);
                        e.printStackTrace();
                    }catch (KafkaException | ExecutionException | InterruptedException e){
                        // kafkaMessages.remove( poID ); we want to keep the message so as to give it another chance
                        uncommitedMessages.remove( poID );

                       //Do nothing as message was not ack'ed to the queue
                        System.out.print(" Sending message " + poID + " to topic: " + COMMITTED_ORDERS_TOPIC +
                                " and/or " + UNCOMMITTED_ORDERS_TOPIC + " failed.");

                        e.printStackTrace();
                    }
                }else if ( !uncommitedMessages.containsKey( poID ) ){
                    // this block ensures that the messages are retained in Kafka even if this app crashes
                    System.out.print("Press any key to publish to uncommitted topic " + poID + ". . .");
                    scanner.nextLine(); // Wait for user input
                    try {
                        //publish(UNCOMMITTED_ORDERS_TOPIC, poID, kafkaMessages.get(poID));

                        task.publish(KAFKA_BOOTSTRAP_SERVERS,
                                SCHEMA_REGISTRY_URL,
                                KAFKA_CLIENT_ID,
                                UNCOMMITTED_ORDERS_TOPIC,
                                poID,
                                kafkaMessages.get(poID));

                        uncommitedMessages.put(poID, kafkaMessages.get(poID));
                        System.out.println("done.");
                    }catch (KafkaException | ExecutionException | InterruptedException e ){
                        System.out.print(" Sending message " + poID + " to topic: " + UNCOMMITTED_ORDERS_TOPIC + " failed.");
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    public static void main(String[] args) {
        uncommitedMessages = new CommonUtilityTask().loadUnprocessedMessagesFromLastSession(
                KAFKA_BOOTSTRAP_SERVERS,
                SCHEMA_REGISTRY_URL,
                UNCOMMITTED_ORDERS_TOPIC,
                KAFKA_CONSUMER_GROUP_ID,
                KAFKA_CLIENT_ID);

        syncReader.start();
    }
}

