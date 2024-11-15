package com.sanjaygarde.kafka2pc.server.reconcliliation;

import com.ecom.supplychain.PurchaseOrder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjaygarde.kafka2pc.CommonUtilityTask;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.config.ConfigResource;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class KafkaRecordsReconJob {

    private static KafkaRecordsReconJob reconJob = new KafkaRecordsReconJob();

    private static String BOOTSTRAP_SERVER_1;
    private static String BOOTSTRAP_SERVER_2;
    private static String MESSAGE_STAGING_TOPIC;

    private static String COMMITTED_MESSAGE_TOPIC;
    private static String UNCOMMITTED_MESSAGE_TOPIC;

    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";

    private static  String SCHEMA_REGISTRY_CLUSTER_1_URL = "http://localhost:8081";

    private static  String SCHEMA_REGISTRY_CLUSTER_2_URL = "http://localhost:8081";

    private static final String CONSUMER_GROUP_ID = "KAFKA_RECONCILIATION_JOB" ;

    private static final String CLIENT_ID = "KAFKA_RECONCILIATION_JOB_CLIENT_1";

    private static final ExecutorService executorService = Executors.newFixedThreadPool(3);;
    private static java.util.Hashtable<String, PurchaseOrder> cluster1Records;
    private static java.util.Hashtable<String, PurchaseOrder> cluster2Records;

    private static final Hashtable<String, PurchaseOrder> cluster1UncommitedMessages = new Hashtable<>();
    private static final Hashtable<String, PurchaseOrder> cluster2UncommitedMessages = new Hashtable<>();


    public void loadUnprocessedMessagesFromLastSession(String bootstrapServer, String schemaRegistryURL, Hashtable<String, PurchaseOrder> records){
        HashMap<String, PurchaseOrder> tombstonedMessages = new HashMap<>();
        HashMap<String, PurchaseOrder> non_tombstonedMessages = new HashMap<>();

        try (Consumer<String, PurchaseOrder> consumer = //createKafkaConsumer(bootstrapServer, schemaRegistryURL, UNCOMMITTED_MESSAGE_TOPIC) ) {
                                    new CommonUtilityTask().createKafkaConsumer(bootstrapServer,
                                            schemaRegistryURL,
                                            UNCOMMITTED_MESSAGE_TOPIC,
                                            CONSUMER_GROUP_ID,
                                            CLIENT_ID,
                                            true)){
            //consumer.subscribe(Collections.singletonList(MESSAGE_STAGING_TOPIC));

            boolean keepConsuming = true;

            while (keepConsuming) {
                ConsumerRecords<String, PurchaseOrder> recordsList = consumer.poll(Duration.ofMillis(100));

                recordsList.forEach(record -> {
                    if ((record.value() == null)) {
                        tombstonedMessages.put(record.key(), record.value());
                    } else {
                        non_tombstonedMessages.put(record.key(), record.value());
                    }
                });

                Iterator<Map.Entry<String, PurchaseOrder>> iterator = tombstonedMessages.entrySet().iterator();

                while (iterator.hasNext()) {
                    Map.Entry<String, PurchaseOrder> entry = iterator.next();
                    String key = entry.getKey();

                    if (non_tombstonedMessages.containsKey(key)) {
                        iterator.remove(); // Remove from tombstonedMessages
                        non_tombstonedMessages.remove(key); // Remove from non_tombstonedMessages
                    }
                }

                Map<TopicPartition, Long> endOffsets = consumer.endOffsets(consumer.assignment());
                keepConsuming = recordsList.partitions().stream().anyMatch(partition -> {
                    long endOffset = endOffsets.get(partition);
                    long currentOffset = consumer.position(partition);
                    return currentOffset < endOffset;
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        records.putAll(non_tombstonedMessages);
    }

    public void start() {
        executorService.submit(() -> consumeFromKafka(BOOTSTRAP_SERVER_1, SCHEMA_REGISTRY_CLUSTER_1_URL, MESSAGE_STAGING_TOPIC, cluster1Records));
        executorService.submit(() -> consumeFromKafka(BOOTSTRAP_SERVER_2, SCHEMA_REGISTRY_CLUSTER_2_URL, MESSAGE_STAGING_TOPIC, cluster2Records));

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

    private static void consumeFromKafka(String bootstrapServer, String schemaRegistryURL, String topic, Hashtable<String, PurchaseOrder> records) {
        System.out.println("Started consuming messages from " + bootstrapServer + " topic: " + topic);

        try (Consumer<String, PurchaseOrder> consumer = //createKafkaConsumer(bootstrapServer, schemaRegistryURL, topic) ) {
                                        new CommonUtilityTask().createKafkaConsumer(bootstrapServer,
                                                schemaRegistryURL,
                                                topic,
                                                CONSUMER_GROUP_ID,
                                                CLIENT_ID,
                                                true)){

            consumer.subscribe(Collections.singletonList(MESSAGE_STAGING_TOPIC));

            while (true) {
                ConsumerRecords<String, PurchaseOrder> recordsList = consumer.poll(Duration.ofMillis(100));
                recordsList.forEach(record -> records.put(record.key(), record.value()));

                consumer.commitSync();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void compareAndTransfer() {
        CommonUtilityTask task = new CommonUtilityTask();

        while (true) {
            //This sleep is a very smart move and dangerous too what if this job crashes, there will be data loss
            try {
                TimeUnit.SECONDS.sleep(5); // Adjust the interval as needed
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            System.out.println("Processing " + cluster1Records.size() + " messages from cluster 1:" + BOOTSTRAP_SERVER_1) ;

            //for (String aKey : cluster1Records.keySet()) {
            Iterator<String> cluster1RecordsIter = cluster1Records.keySet().iterator();
            while (cluster1RecordsIter.hasNext()){
                String aKey = cluster1RecordsIter.next();

                if (cluster2Records.containsKey(aKey)) {
                    System.out.println( "Matching record found for key: " + aKey );
                    PurchaseOrder value = cluster1Records.get(aKey);

                    try {
                        //publish(BOOTSTRAP_SERVER_1, COMMITTED_MESSAGE_TOPIC, aKey, value);
                        task.publish(BOOTSTRAP_SERVER_1,
                                SCHEMA_REGISTRY_CLUSTER_1_URL,
                                CLIENT_ID,
                                COMMITTED_MESSAGE_TOPIC,
                                aKey,
                                value);
                        cluster1RecordsIter.remove();

                        try {
                            //publish(BOOTSTRAP_SERVER_2, COMMITTED_MESSAGE_TOPIC, aKey, value);

                            task.publish(BOOTSTRAP_SERVER_2,
                                    SCHEMA_REGISTRY_CLUSTER_2_URL,
                                    CLIENT_ID,
                                    COMMITTED_MESSAGE_TOPIC,
                                    aKey,
                                    value);

                            cluster2Records.remove(aKey);
                        } catch (KafkaException ke) {
                            if (!cluster1UncommitedMessages.containsKey(aKey)) {
                                //publish(BOOTSTRAP_SERVER_1, UNCOMMITTED_MESSAGE_TOPIC, aKey, value);

                                task.publish(BOOTSTRAP_SERVER_1,
                                        SCHEMA_REGISTRY_CLUSTER_1_URL,
                                        CLIENT_ID,
                                        UNCOMMITTED_MESSAGE_TOPIC,
                                        aKey,
                                        value);

                                cluster1UncommitedMessages.put(aKey, value);
                                throw ke;
                            }
                        }

                        if (cluster1UncommitedMessages.containsKey(aKey)) {
                            //send tombstone message to UNCOMMITTED_MESSAGE_TOPIC
                            //publish(BOOTSTRAP_SERVER_1, UNCOMMITTED_MESSAGE_TOPIC, aKey, null);

                            task.publish(BOOTSTRAP_SERVER_1,
                                    SCHEMA_REGISTRY_CLUSTER_1_URL,
                                    CLIENT_ID,
                                    UNCOMMITTED_MESSAGE_TOPIC,
                                    aKey,
                                    null);

                            cluster1UncommitedMessages.remove(aKey);
                            System.out.println("Sent tombstone message for " + aKey + " to " + UNCOMMITTED_MESSAGE_TOPIC + " on " + BOOTSTRAP_SERVER_1);
                        } else if (cluster2UncommitedMessages.containsKey(aKey)) {
                            //send tombstone message to UNCOMMITTED_MESSAGE_TOPIC
                            //publish(BOOTSTRAP_SERVER_2, UNCOMMITTED_MESSAGE_TOPIC, aKey, null);

                            task.publish(BOOTSTRAP_SERVER_2,
                                    SCHEMA_REGISTRY_CLUSTER_2_URL,
                                    CLIENT_ID,
                                    UNCOMMITTED_MESSAGE_TOPIC,
                                    aKey,
                                    null);

                            cluster2UncommitedMessages.remove(aKey);
                            System.out.println("Sent tombstone message for  " + aKey + " to " + UNCOMMITTED_MESSAGE_TOPIC + " on " + BOOTSTRAP_SERVER_2);
                        }

                        System.out.println("Transferred " + aKey + " to " + COMMITTED_MESSAGE_TOPIC + " on " + BOOTSTRAP_SERVER_1 + " and " + BOOTSTRAP_SERVER_2);

                    } catch (KafkaException | ExecutionException | InterruptedException ke) {
                        ke.printStackTrace();
                        System.out.println("Matching message " + aKey + " could not be published to one or more topics.");
                    }
                }else{
                        // Record only present in cluster1Records
                        System.out.println(aKey + " not Found  in " + BOOTSTRAP_SERVER_2 + ", transferring to " + UNCOMMITTED_MESSAGE_TOPIC + " topic on " + BOOTSTRAP_SERVER_1);
                        PurchaseOrder po1 = cluster1Records.get(aKey);
                        try {
                            //publish(BOOTSTRAP_SERVER_1, UNCOMMITTED_MESSAGE_TOPIC, aKey, po1);

                            task.publish(BOOTSTRAP_SERVER_1,
                                    SCHEMA_REGISTRY_CLUSTER_1_URL,
                                    CLIENT_ID,
                                    UNCOMMITTED_MESSAGE_TOPIC,
                                    aKey,
                                    po1);

                            cluster1UncommitedMessages.put(aKey, po1);
                        } catch (KafkaException | ExecutionException | InterruptedException e) {
                            e.printStackTrace();
                            System.out.println("Unmatched message from cluster 1" + aKey + " could not be transferred to " + UNCOMMITTED_MESSAGE_TOPIC);
                        }
                }

                System.out.println("Record count in cluster 1: (" + BOOTSTRAP_SERVER_1 + "): " + cluster1Records.size());
                System.out.println("Record count in cluster 2: (" + BOOTSTRAP_SERVER_2 + "): " + cluster2Records.size());
            }

            System.out.println("Processed " + cluster1Records.size() + " records from " + BOOTSTRAP_SERVER_1 + " topic:" + MESSAGE_STAGING_TOPIC);

            System.out.println("Processing " + cluster2Records.size() + " messages from " + BOOTSTRAP_SERVER_2 + " topic:" + MESSAGE_STAGING_TOPIC) ;
            for (String bKey : cluster2Records.keySet()) {
                if (!cluster1Records.containsKey(bKey)) {
                    // Record only present in cluster2Records
                    PurchaseOrder po2 = cluster2Records.get(bKey);

                    try {
                        if(!cluster2UncommitedMessages.containsKey(bKey)) {
                            System.out.println(bKey + " not Found  in cluster 1, transferring to " +  UNCOMMITTED_MESSAGE_TOPIC   + " topic on cluster 2");
                            //publish(BOOTSTRAP_SERVER_2, UNCOMMITTED_MESSAGE_TOPIC, bKey, po2);

                            task.publish(BOOTSTRAP_SERVER_2,
                                    SCHEMA_REGISTRY_CLUSTER_2_URL,
                                    CLIENT_ID,
                                    UNCOMMITTED_MESSAGE_TOPIC,
                                    bKey,
                                    po2);


                            cluster2UncommitedMessages.put(bKey, po2);
                        }
                    }catch(KafkaException | ExecutionException | InterruptedException e) {
                        e.printStackTrace();
                        System.out.println("Unmatched message from cluster 2" + bKey + " could not be transferred to " + UNCOMMITTED_MESSAGE_TOPIC);
                    }
                }

                System.out.println("Processed " + cluster2Records.size() + " records from " + BOOTSTRAP_SERVER_2 + " topic:" + MESSAGE_STAGING_TOPIC);
            }
        }
    }

    private static void parseJsonFile(String filePath) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            // Parse the JSON file into a Map
            Map<String, String> params = objectMapper.readValue(new File(filePath), Map.class);

            // Assign values to class members
            BOOTSTRAP_SERVER_1 = params.get("bootstrapServer1");
            SCHEMA_REGISTRY_CLUSTER_1_URL = params.get("schemaRegistryURL1");
            MESSAGE_STAGING_TOPIC = params.get("stagingTopic");
            BOOTSTRAP_SERVER_2 = params.get("bootstrapServer2");
            SCHEMA_REGISTRY_CLUSTER_2_URL = params.get("schemaRegistryURL2");

            COMMITTED_MESSAGE_TOPIC = "commited_" + MESSAGE_STAGING_TOPIC;
            UNCOMMITTED_MESSAGE_TOPIC = "uncommitted_" + MESSAGE_STAGING_TOPIC;
        } catch ( IOException e) {
            System.err.println("Error reading the JSON file: " + e.getMessage());
            System.exit(1);
        }
    }

    private static void setCleanupPolicy(String topicName){
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // Create the AdminClient
        try (AdminClient adminClient = AdminClient.create(props)) {

            // Define the config resource for the topic
            ConfigResource configResource = new ConfigResource(Type.TOPIC, topicName);

            // Create the new config entry for retention policy
            ConfigEntry retentionPolicyEntry = new ConfigEntry("cleanup.policy", "compact");

            // Create the AlterConfigOp for the new config entry
            AlterConfigOp alterConfigOp = new AlterConfigOp(retentionPolicyEntry, AlterConfigOp.OpType.SET);

            // Apply the new config to the topic
            adminClient.incrementalAlterConfigs(Collections.singletonMap(configResource, Collections.singletonList(alterConfigOp))).all().get();

            System.out.println("Retention policy set to 'compact' for topic: " + topicName);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {

        if (args.length != 1) {
            System.err.println("Usage: java KafkaRecordsReconJob <path-to-json-file>");
            System.exit(1);
        }

        parseJsonFile( args[0] );
        setCleanupPolicy( UNCOMMITTED_MESSAGE_TOPIC );

        CommonUtilityTask ct = new CommonUtilityTask();

       cluster1Records = ct.loadUnprocessedMessagesFromLastSession(
                                                BOOTSTRAP_SERVER_1,
                                                SCHEMA_REGISTRY_CLUSTER_1_URL,
                                                UNCOMMITTED_MESSAGE_TOPIC,
                                                CONSUMER_GROUP_ID,
                                                CLIENT_ID);

        cluster2Records = ct.loadUnprocessedMessagesFromLastSession(
                                                BOOTSTRAP_SERVER_2,
                                                SCHEMA_REGISTRY_CLUSTER_2_URL,
                                                UNCOMMITTED_MESSAGE_TOPIC,
                                                CONSUMER_GROUP_ID,
                                                CLIENT_ID);

//        reconJob.loadUnprocessedMessagesFromLastSession(BOOTSTRAP_SERVER_1, SCHEMA_REGISTRY_CLUSTER_1_URL, cluster1Records);
//        reconJob.loadUnprocessedMessagesFromLastSession(BOOTSTRAP_SERVER_2, SCHEMA_REGISTRY_CLUSTER_2_URL, cluster2Records);

        reconJob.start();
    }
}

