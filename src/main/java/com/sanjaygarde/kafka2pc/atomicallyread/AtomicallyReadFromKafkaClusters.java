package com.sanjaygarde.kafka2pc.atomicallyread;

public class AtomicallyReadFromKafkaClusters {
    //this is not a valid case because unlike message queues a message does not disappear from kafka if it was read successfully.
    // So if a consumer could not read, it can always re-read.
    //Making consumer read data from two kafka clusters at the same time does not make sense and so this is not a valid use case
    //Add this to the slide deck not here
}
