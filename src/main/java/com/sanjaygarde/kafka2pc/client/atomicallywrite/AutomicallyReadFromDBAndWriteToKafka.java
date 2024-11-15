package com.sanjaygarde.kafka2pc.client.atomicallywrite;

//So there is no need to atomically read from DB, there is nothing gained by reading it atomically.
////this is not a valid case because unlike message queues a message does not disappear from DB if it was read successfully.
public class AutomicallyReadFromDBAndWriteToKafka {
}
