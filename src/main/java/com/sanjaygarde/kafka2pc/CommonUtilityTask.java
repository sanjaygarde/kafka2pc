package com.sanjaygarde.kafka2pc;

import com.ecom.supplychain.PurchaseOrder;
import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class CommonUtilityTask {

    public KafkaProducer<String, PurchaseOrder> createKafkaProducer(String boostrapServer,
                                                                    String schemaRegistryURL,
                                                                    String clientID){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServer);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientID);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer");

        return new KafkaProducer<>(props);
    }


    public void publish(String boostrapServer,
                         String schemaRegistryURL,
                         String clientID,
                         String topicToPublish,
                         String poID,
                         PurchaseOrder po) throws ExecutionException, InterruptedException {
        //System.out.print("Publishing " + poID + " to " + boostrapServer  +  " topic " + topicToPublish);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServer);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientID);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer");

        KafkaProducer<String, PurchaseOrder> producer = new KafkaProducer<>(props);
        ProducerRecord<String, PurchaseOrder> record = new ProducerRecord<String, PurchaseOrder>(topicToPublish, poID, po);

        producer.send(record).get(); //sync send
        producer.close();

        //System.out.print("done");
    }

    public Consumer<String, JsonNode> createKafkaConsumer(String bootstrapServer,
                                                           String schemaRegistryURL,
                                                           String groupId,
                                                           String kafkaClientID,
                                                           boolean autocommit) {

        Properties props = new Properties();
        // Add additional properties.
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, kafkaClientID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer");
        props.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, PurchaseOrder.class.getName());

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autocommit);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        return new KafkaConsumer<>(props);
    }

    public KafkaConsumer<String, PurchaseOrder> createKafkaConsumer(String bootstrapServer,
                                                                            String schemaRegistryURL,
                                                                            String topicName,
                                                                            String consumerGroup,
                                                                            String clientID,
                                                                            boolean autocommit) {
        Properties props = new Properties();

        // Add additional properties.
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer");
        props.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, PurchaseOrder.class.getName());

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autocommit );
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        KafkaConsumer<String, PurchaseOrder> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(java.util.Arrays.asList(topicName));
        return consumer;
    }


    public Hashtable<String, PurchaseOrder> loadUnprocessedMessagesFromLastSession(String bootstrapServer,
                                                       String schemaRegistryURL,
                                                       String uncommittedMessageTopic,
                                                       String consumerGroup,
                                                       String clientID){
        HashMap<String, PurchaseOrder> tombstonedMessages = new HashMap<>();
        HashMap<String, PurchaseOrder> nonTombstonedMessages = new HashMap<>();

        try (Consumer<String, PurchaseOrder> consumer = createKafkaConsumer(bootstrapServer,
                                                        schemaRegistryURL,
                                                        consumerGroup,
                                                        clientID,
                                                        uncommittedMessageTopic,
                                                        true) ) {
            //consumer.subscribe(Collections.singletonList(MESSAGE_STAGING_TOPIC));

            boolean keepConsuming = true;

            while (keepConsuming) {
                ConsumerRecords<String, PurchaseOrder> recordsList = consumer.poll(Duration.ofMillis(100));

                recordsList.forEach(record -> {
                    if ((record.value() == null)) {
                        tombstonedMessages.put(record.key(), record.value());
                    } else {
                        nonTombstonedMessages.put(record.key(), record.value());
                    }
                });

                Iterator<Map.Entry<String, PurchaseOrder>> iterator = tombstonedMessages.entrySet().iterator();

                while (iterator.hasNext()) {
                    Map.Entry<String, PurchaseOrder> entry = iterator.next();
                    String key = entry.getKey();

                    if (nonTombstonedMessages.containsKey(key)) {
                        iterator.remove(); // Remove from tombstonedMessages
                        nonTombstonedMessages.remove(key); // Remove from non_tombstonedMessages
                    }
                }

                Map<TopicPartition, Long> endOffsets = consumer.endOffsets(consumer.assignment());
                keepConsuming = recordsList.partitions().stream().anyMatch(partition -> {
                    long endOffset = endOffsets.get(partition);
                    long currentOffset = consumer.position(partition);
                    return currentOffset < endOffset;
                });
            }
        }

        Hashtable<String, PurchaseOrder> records = new Hashtable<>();
        records.putAll(nonTombstonedMessages);
        return records;
    }
}
