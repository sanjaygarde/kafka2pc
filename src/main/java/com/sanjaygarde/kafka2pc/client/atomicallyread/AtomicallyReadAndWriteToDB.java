package com.sanjaygarde.kafka2pc.client.atomicallyread;

import com.sanjaygarde.kafka2pc.CommonUtilityTask;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

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

	private void readAndWriteInSynch(){
		Map<TopicPartition, Long> lastPolledOffsets = new HashMap<>();
		Scanner scanner = new Scanner(System.in);


		try (final Consumer<String, JsonNode> kafkaConsumer =
					 // = createKafkaConsumer(KAFKA_BOOTSTRAP_SERVERS, KAFKA_CONSUMER_GROUP_ID,  KAFKA_CLIENT_ID, ORDERS_TOPIC)) {
					 new CommonUtilityTask().createKafkaConsumer(KAFKA_BOOTSTRAP_SERVERS,
							 SCHEMA_REGISTRY_URL,
							 KAFKA_CONSUMER_GROUP_ID,
							 KAFKA_CLIENT_ID, false)){

			kafkaConsumer.subscribe(Arrays.asList( ORDERS_TOPIC ));

			while (true) {
				ConsumerRecords<String, JsonNode> records = kafkaConsumer.poll(Duration.ofMillis(100));

				//record by record commit is a bad idea, it defeats the scalability of Kafka
				// if possible use batch approach.
				for (ConsumerRecord<String, JsonNode>record : records) {

					TopicPartition topicPartition = new  TopicPartition( record.topic(), record.partition() );
					OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata( record.offset() );
					HashMap<TopicPartition, OffsetAndMetadata> partitionAndMetadata = new HashMap<>();
					partitionAndMetadata.put(topicPartition, offsetAndMetadata);

					String key = record.key();
					PurchaseOrder po = new ObjectMapper().convertValue(record.value(), new TypeReference<PurchaseOrder>(){});

					System.out.println("\nProcessing record: " + po.getOrderId());
					System.out.println( "--> " + record.offset() );

					Connection dbConn = null;

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
						System.out.print("Executing the insert statement . . . ");
						preparedStatement.executeUpdate();

						System.out.println("done. Press enter to commit offsets to Kafka: ");
						scanner.nextLine();
						kafkaConsumer.commitSync( partitionAndMetadata );
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
							//do nothing conn.setAutoCommit is false
							throw new RuntimeException(ex);
						}

						System.out.println("done.");
						System.out.println("==============================================");

						break;//start over, poll kafka again

					} catch (SQLException e) {// These catch blocks handle transactions, handle with care
						//This means database commit failed after having committed the offsets
						//so we manually reset the offsets to prevent data loss
						e.printStackTrace();
						System.out.println("==============================================");
						System.out.println("Database commit failed, resetting consumer offsets . . . ");

						kafkaConsumer.seek(topicPartition, record.offset() - 1 );

						System.out.println("done.");
						System.out.println("==============================================");

						break;//start over, poll kafka again
					}
				}
			}
		}catch (KafkaException | SQLException e ){ // this catch block does not handle transaction, maybe Kafka or DB is down
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		synchWriter.readAndWriteInSynch();
	}
}
