package com.sanjaygarde.kafka2pc.server.reconcliliation;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RetriableException;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/*
TODO:

1) Replace Sop with a logger


 */

public class KafkaRecordsReconJob {
    private final String bootstrapServer1;
    private final String bootstrapServer2;
    private final String topic;

    private final String groupID = "ReconJob" ;
    private final ExecutorService executorService;
    private final java.util.Hashtable<String, String> cluster1Records;
    private final java.util.Hashtable<String, String> cluster2Records;

    public KafkaRecordsReconJob(String bootstrapServer1, String bootstrapServer2, String topic) {
        this.bootstrapServer1 = bootstrapServer1;
        this.bootstrapServer2 = bootstrapServer2;
        this.topic = topic;
        this.executorService = Executors.newFixedThreadPool(3);
        this.cluster1Records = new java.util.Hashtable<>();
        this.cluster2Records = new java.util.Hashtable<>();
    }

    public void start() {
        executorService.submit(() -> consumeFromKafka(bootstrapServer1, cluster1Records));
        executorService.submit(() -> consumeFromKafka(bootstrapServer2, cluster2Records));

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

    private static KafkaConsumer<String, String> createKafkaConsumer(String bootstrapServer, String groupId, String topicName) {
        Properties props = new Properties();
        // Add additional properties.
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        //props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, groupId + "_client");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        //for handling transactions
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(java.util.Arrays.asList(topicName));
        return consumer;
    }

    private void consumeFromKafka(String bootstrapServer, java.util.Hashtable<String, String> records) {
        try (Consumer<String, String> consumer = createKafkaConsumer(bootstrapServer, groupID, topic) ) {
            consumer.subscribe(Collections.singletonList(topic));

            while (true) {
                ConsumerRecords<String, String> recordsList = consumer.poll(Duration.ofMillis(100));
                recordsList.forEach(record -> records.put(record.key(), record.value()));
                //System.out.println("Read from " + bootstrapServer + " added to hashtable" + records.toString());

                //Note *******
                //This can be a problem, if the recon job crashes before adding data to read_ready topic then the committed records will be missed
                //Ideally commit can only happen after records are successfully moved to read_ready topic
                // so compareAndTransfer should be called from here not in a separate thread
                //Note *******
                consumer.commitSync();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void compareAndTransfer() {
        while (true) {

            //This sleep is a very smart move
            try {
                TimeUnit.SECONDS.sleep(5); // Adjust the interval as needed
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            for (String aKey : cluster1Records.keySet()) {
                System.out.println("is this key: " + aKey + "in cluster2Records: " + cluster2Records.containsKey(aKey));

                if (cluster2Records.containsKey(aKey)) {
                    String value = cluster1Records.get(aKey);

                    transferRecords(aKey, value, bootstrapServer1);
                    transferRecords(aKey, value, bootstrapServer2);

                    System.out.println( "Transferred " + aKey + " to " + topic + "-read-ready topic of " + bootstrapServer1 + " and " + bootstrapServer2 );
                }
                else {
                    // Record only present in cluster1Records
                    System.out.println( aKey + " Not Found  in cluster2Records, transferring to exclude topic.");
                    transferRecordToExcludeTopic(aKey, cluster1Records.get(aKey), bootstrapServer1);
                }
            }

            System.out.println("Processed " + cluster1Records.size() + " records from " + bootstrapServer1 + " topic:" + topic);

            for (String bKey : cluster2Records.keySet()) {
                if (!cluster1Records.containsKey(bKey)) {
                    // Record only present in cluster2Records
                    System.out.println( bKey + " Not Found  in cluster1Records, transferring to exclude topic.");
                    transferRecordToExcludeTopic(bKey, cluster2Records.get(bKey), bootstrapServer2);
                }
            }

            //commit here

            System.out.println("Processed " + cluster2Records.size() + " records from " + bootstrapServer2 + " topic:" + topic);

            if( !cluster1Records.isEmpty() )
                cluster1Records.clear();

            if( !cluster2Records.isEmpty() )
                cluster2Records.clear();

        }
    }

    private static KafkaProducer<String, String> createKafkaProducer(String bootstrapServer) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<>(props);

    }

    private void transferRecords(String key, String value, String bootstrapServer) {

        try (Producer<String, String> producer = createKafkaProducer(bootstrapServer)) {
            String newTopic = topic + "-read-ready";
            producer.send(new ProducerRecord<>(newTopic, key, value));

            //Ideally you should get an ack and only then commit the last batch

            System.out.println(key + "sent to read ready topic on " + bootstrapServer);
        } catch (Exception e) {
            // Handle failure to transfer record
            e.printStackTrace();
            transferRecordToExcludeTopic(key, value, bootstrapServer);
        }
    }

    private void transferRecordToExcludeTopic(String key, String value, String bootstrapServer) {

        try (Producer<String, String> producer = createKafkaProducer(bootstrapServer)) {
            String newTopic = topic + "-exclude";
            producer.send(new ProducerRecord<>(newTopic, key, value));
            System.out.println("transferred " + key + " to " + newTopic + " on " + bootstrapServer);
        }catch(RetriableException re){
            re.printStackTrace();
            //do something here if need be
        }catch (Exception e) {
            e.printStackTrace();
            // Handle failure to transfer record to exclude topic
        }
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: KafkaRecordsReconJob <bootstrapServer1> <bootstrapServer2> <topic>");
            System.exit(1);
        }

        String bootstrapServer1 = args[0];
        String bootstrapServer2 = args[1];
        String topic = args[2];

        KafkaRecordsReconJob reconJob = new KafkaRecordsReconJob(bootstrapServer1, bootstrapServer2, topic);
        reconJob.start();
    }
}

