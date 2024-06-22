package com.sanjaygarde.kafka2pc.atomicallyread;

import com.ecom.supplychain.PurchaseOrder;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/*
TODO:

1) Replace Sop with a logger


 */

public class AtomicallyReadFromKafkaAndMQ {

    private static final AtomicallyReadFromKafkaAndMQ syncReader = new AtomicallyReadFromKafkaAndMQ();
    private static final String kafkaBootstrapServers = "localhost:9092";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";

    private static final String amqpHost = "amqp://localhost";
    private static final String consumerGroupID = "MQCrossReader24";

    private static final String kafkaClientID= "MQCrossReader24";

    private static final String ordersTopic = "orders";
    private static final String ordersQueue = "orders";
    private static Channel channel;
    private static final String committedOrdersTopic = "committed_orders";
    private static final String uncommittedOrdersTopic = "uncommitted_orders";

    //TODO: Turn orderId to String
    // All below should be static, this class is a singleton
    private Hashtable<String, PurchaseOrder> kafkaMessages;
    private Hashtable<String, POWithDeliveryTag> mqMessages;

    private Hashtable<String, PurchaseOrder> uncommitedMessages;

    //this is no longer needed
    private Hashtable<TopicPartition, OffsetAndMetadata> ordersOffsets;

    private final ExecutorService executorService;

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

    private AtomicallyReadFromKafkaAndMQ(){
        this.executorService = Executors.newFixedThreadPool(3);

        //hashtables because we want to be thread safe (at the cost of some performance penalty for which kafka more than compensates)

        this.kafkaMessages = new Hashtable<>();
        this.mqMessages = new Hashtable<>();
        this.ordersOffsets =  new Hashtable<>(); //This will not be needed anymore
        this.uncommitedMessages = new Hashtable<>();
    }

    public void start() {
        executorService.submit(() -> consumeFromKafka(kafkaBootstrapServers, kafkaMessages));
        executorService.submit(() -> consumeFromQueue(amqpHost, ordersQueue));

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

    private Consumer<String, JsonNode> createKafkaConsumer(String bootstrapServer, String groupId, String topicName) {

        Properties props = new Properties();
        // Add additional properties.
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, kafkaClientID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer");
        props.put(io.confluent.kafka.serializers.KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, PurchaseOrder.class.getName());

        //for handling transactions, this is not needed as we are using a staging topic and a committed topic
        //props.put("ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG", "false");
        //props.put("enable.auto.commit", false); This is no longer needed
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        return new KafkaConsumer<>(props);
    }

    private void consumeFromKafka(String bootstrapServer, Hashtable<String, PurchaseOrder> poRecords) {
        Consumer<String, JsonNode> consumer = createKafkaConsumer(kafkaBootstrapServers, consumerGroupID, ordersTopic);
        consumer.subscribe(Arrays.asList(ordersTopic));

        while (true) {
            consumer.poll(Duration.ofMillis(100)).forEach(record -> {
                System.out.println("Reading from kafka topic: " + ordersTopic );
                PurchaseOrder po = new ObjectMapper().convertValue(record.value(), new TypeReference<PurchaseOrder>() {
                });
                System.out.println("po: " + po.getOrderId());
                kafkaMessages.put(po.getOrderId(), po);
                System.out.println("kafkaMessages.size():" + kafkaMessages.size());

                //This is no longer needed as autocommit is true
                //String topic = record.topic();
//                if (ordersTopic.equals(topic)) {
//                    // Track offset to commit for "orders" topic
//                    TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
//                    OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(record.offset() + 1, "no metadata");
//                    ordersOffsets.put(topicPartition, offsetAndMetadata);
//                }
            });

            //This is no longer needed as autocommit is true
//            if (!ordersOffsets.isEmpty()) {
//                consumer.commitSync(ordersOffsets);
//            }
        }
    }

    private void consumeFromQueue(String amqpHost, String ordersQueue){
        System.out.println("Consuming from message queue: " + ordersQueue);

        ConnectionFactory factory = new ConnectionFactory();
        try {
            factory.setUri( amqpHost );  // Replace with your RabbitMQ server URI
        } catch (URISyntaxException | KeyManagementException | NoSuchAlgorithmException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }


        try  {
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
            channel.basicConsume("orders", false, consumer);

        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private KafkaProducer<String, PurchaseOrder> createKafkaProducer( ){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, kafkaClientID);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer");

       return new KafkaProducer<>(props);
    }

    private void publish(String topicToPublish, String poID, PurchaseOrder po){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, kafkaClientID);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer");

        KafkaProducer<String, PurchaseOrder> producer = new KafkaProducer<>(props);
        ProducerRecord<String, PurchaseOrder> record = new ProducerRecord<String, PurchaseOrder>(topicToPublish, poID, po);

        try {
            producer.send(record).get(); //sync send
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        producer.close();
    }

    private void compareAndTransfer() {
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
                    System.out.println("publishing to committed topic " + poID);

                    KafkaProducer<String, PurchaseOrder> producer = createKafkaProducer();
                    ProducerRecord<String, PurchaseOrder> committedRecord = new ProducerRecord<String, PurchaseOrder>(committedOrdersTopic, poID, po);

                    //the lines BELOW in the try block should be all or none
                    try {
                        producer.send(committedRecord).get(); //sync send
                        kafkaMessages.remove(poID);

                        POWithDeliveryTag poDT = mqMessages.get(poID);
                        try {
                            channel.basicAck(poDT.getDeliveryTag(), false);
                            mqMessages.remove(poID);

                            System.out.println("publishing tombstone message to uncommitted topic for:" + poID);
                            //send a tombstone message to the uncommittedOrdersTopic
                            ProducerRecord<String, PurchaseOrder> uncommittedRecord = new ProducerRecord<String, PurchaseOrder>(uncommittedOrdersTopic, poID, null );
                            producer.send(uncommittedRecord).get(); //sync send

                            uncommitedMessages.remove(poID);
                        } catch (IOException e) {
                            //ack could not be sent to message queue
                            //log the condition and stop the job
                            // Maybe try one more time
                            // kafkaMessages.put(poID, po) so this ack can be processed in the next cycle, great idea !!!
                            e.printStackTrace();
                            throw new RuntimeException(e);
                        }catch (InterruptedException | ExecutionException  e){
                            // message could not be produced to uncommittedOrdersTopic
                            //retries should take care of this
                            e.printStackTrace();
                        }

                    } catch (InterruptedException | ExecutionException  e) {
                        // message could not be produced to committedOrdersTopic
                        //retries should take care of this
                        e.printStackTrace();
                    }
                }else if ( !uncommitedMessages.containsKey( poID ) ){
                    System.out.println("publishing to uncommitted topic " + poID);
                    publish( uncommittedOrdersTopic, poID, kafkaMessages.get(poID) );
                    uncommitedMessages.put( poID, kafkaMessages.get(poID) );
                }
            }
        }
    }

    public static void main(String[] args) {
        syncReader.start();
    }
}

