package com.sanjaygarde.kafka2pc.atomicallyread;

import com.ecom.supplychain.PurchaseOrder;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.clients.consumer.*;

import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Properties;
import java.util.Collections;

public class TestTopicConsumer {
    public static void main(String args[]){
        final String topic = "orders";

        Properties props = new Properties();
        // Add additional properties.
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "MQCrossReader12");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "MQCrossReader12");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer");
        props.put(io.confluent.kafka.serializers.KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, PurchaseOrder.class.getName());

        //for handling transactions
        //props.put("ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG", "false");
        props.put("enable.auto.commit", false);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        try (final Consumer<String, JsonNode> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            while (true) {
                ConsumerRecords<String, JsonNode> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, JsonNode> record : records) {
                    String key = record.key();

                    ObjectMapper objMapper = new ObjectMapper();
                    PurchaseOrder po = objMapper.convertValue(record.value(), new TypeReference<PurchaseOrder>() {
                    });

                    System.out.println("po.getOrderId()" + po.getOrderId());
                }
            }
        }
    }
}
