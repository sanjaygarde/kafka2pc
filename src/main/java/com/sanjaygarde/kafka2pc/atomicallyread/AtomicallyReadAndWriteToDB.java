package com.sanjaygarde.kafka2pc.atomicallyread;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;

import java.time.Duration;
import java.util.*;
import java.sql.*;

import com.ecom.supplychain.PurchaseOrder;
import org.apache.kafka.common.KafkaException;

public class AtomicallyReadAndWriteToDB {

	private static final String ORDERS_TOPIC = "orders";
	private static final String DB_CONNECTION_URL = "jdbc:mysql://localhost:6603/bankdb";
	private static final String SQL_INSERT_QUERY = "INSERT INTO Orders ( order_id, purchaser_id, item_id, price, quantity, order_date) VALUES (?,?,?,?,?,?)";

	private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
	private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";

	private static final String KAFKA_CONSUMER_GROUP_ID = "DB_CROSS_WRITER";

	private static final String KAFKA_CLIENT_ID = "DB_CROSS_WRITER-1";

	private static final AtomicallyReadAndWriteToDB synchWriter = new AtomicallyReadAndWriteToDB();

	private Consumer<String, JsonNode> createKafkaConsumer(String bootstrapServer, String groupId, String kafkaClientID, String topicName) {

		Properties props = new Properties();
		// Add additional properties.
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.CLIENT_ID_CONFIG, kafkaClientID);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer");
		props.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, PurchaseOrder.class.getName());

		//for handling transactions, this is not needed as we are using a staging topic and a committed topic
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

		return new KafkaConsumer<>(props);
	}

	private void readAndWriteInSynch(){
		Map<TopicPartition, Long> lastPolledOffsets = new HashMap<>();
		Scanner scanner = new Scanner(System.in);

		try (final Consumer<String, JsonNode> kafkaConsumer
					 = createKafkaConsumer(KAFKA_BOOTSTRAP_SERVERS, KAFKA_CONSUMER_GROUP_ID,  KAFKA_CLIENT_ID, ORDERS_TOPIC)) {
			kafkaConsumer.subscribe(Arrays.asList( ORDERS_TOPIC ));

			int recordsPolled = 0;

			while (true) {
				ConsumerRecords<String, JsonNode> records = kafkaConsumer.poll(Duration.ofMillis(100));

				recordsPolled = records.count();

				if (!records.isEmpty()) {
					records.partitions().forEach(partition -> {
						long offset = records.records(partition)
								.get(records.records(partition).size() - 1)
								.offset() + 1;

						lastPolledOffsets.put(partition, offset);
					});
				}

				for (ConsumerRecord<String, JsonNode>record : records) {
					String key = record.key();
					PurchaseOrder po = new ObjectMapper().convertValue(record.value(), new TypeReference<PurchaseOrder>(){});

					System.out.println("\nProcessing record: " + po.getOrderId());

					Connection dbConn = null;

					try {
						dbConn = DriverManager.getConnection(DB_CONNECTION_URL, "root", "my-secret-pw");
						PreparedStatement preparedStatement = dbConn.prepareStatement(SQL_INSERT_QUERY);

						dbConn.setAutoCommit(false);

						preparedStatement.setString(1, po.getOrderId());
						preparedStatement.setString(2, po.getPurchaserId());
						preparedStatement.setString(3, po.getItemId());
						preparedStatement.setDouble(4, po.getPrice());
						preparedStatement.setInt(5, po.getQuantity());
						preparedStatement.setDate(6, po.getOrderDate());

						try { //keep minimal code here, only transactional code below

							System.out.println("Executing the insert statement . . . ");
							preparedStatement.executeUpdate();

							System.out.println("done. Press enter to commit offsets to Kafka: ");
							scanner.nextLine();
							kafkaConsumer.commitSync();
							System.out.println("Offsets committed successfully to Kafka.");

							System.out.println("Press enter to commit insert statement: ");
							scanner.nextLine();
							dbConn.commit();
							System.out.println("Insert statement committed successfully");

						} catch (KafkaException kfe) {// These catch blocks handle transactions, handle with care
							//This means committing offsets at Kafka failed and there was no chance to commit insert at the database
							//so we rollback DB insert
							kfe.printStackTrace();
							System.out.println("==============================================");
							System.out.println("Committing offsets at Kafka failed, rollingback DB insert . . . ");

							try {
								dbConn.rollback();
							} catch (SQLException ex) {
								throw new RuntimeException(ex);
							}

							System.out.println("done.");
							System.out.println("==============================================");
						} catch (SQLException e) {// These catch blocks handle transactions, handle with care
							//This means database commit failed after having committed the offsets
							//so we manually reset the offsets to prevent data loss
							e.printStackTrace();
							System.out.println("==============================================");
							System.out.println("Database commit failed, resetting consumer offsets . . . ");

							int finalRecordsPolled = recordsPolled;
							lastPolledOffsets.forEach((partition, offset) -> {
								System.out.println("offset: " + (offset - 1));
								System.out.println("(offset - finalRecordsPolled): " + (offset - finalRecordsPolled));
								kafkaConsumer.seek(partition, (offset - finalRecordsPolled));
							});

							System.out.println("done.");
							System.out.println("==============================================");
						}
					}catch(SQLException e){ // this catch block does not handle transaction, maybe DB is down
						e.printStackTrace();
					}
				}
			}
		}catch (KafkaException kfe){ // this catch block does not handle transaction, maybe Kafka is down
			kfe.printStackTrace();
		}
	}

	public static void main(String[] args) {
		synchWriter.readAndWriteInSynch();
	}
}
