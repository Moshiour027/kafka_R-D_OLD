import org.apache.kafka.clients.producer.*;

import java.text.*;
import java.util.*;

/*
*
* ProducerApp.java
This source file contains the basic code needed to produce records to an Apache Kafka cluster.
 In this class, you can set the number of records that you would like to send at a time.
 Additionally, this class enables you to establish a sleep timer setting to throttle the rate at
  which records are sent to the cluster. You can also un-comment a line that enables a random sleep timer setting.
The functionality in this class is intended to be similar to the kafka-producer-perf-test.sh shell script found in
 the {KAFKA_HOME}/bin directory.

*
*
* */
public class ProducerApp {

    public static void main(String[] args){

        // Create the Properties class to instantiate the Consumer with the desired settings:
        /*
        *
        * A Properties class containing the required configuration settings for a Kafka
         * Producer needs to be created. For convenience all of the relevant configuration settings
         * for this course have been provided using the default settings.
         * It is encouraged that you experiment with different settings in conjunction with the broker settings to
          * understand how the behavior changes.

        *
        * */
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092, localhost:9093");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "");
        props.put("buffer.memory", 33554432);
        props.put("compression.type", "none");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("client.id", "");
        props.put("linger.ms", 0);
        props.put("max.block.ms", 60000);
        props.put("max.request.size", 1048576);
        props.put("partitioner.class", "org.apache.kafka.clients.producer.internals.DefaultPartitioner");
        props.put("request.timeout.ms", 30000);
        props.put("timeout.ms", 30000);
        props.put("max.in.flight.requests.per.connection", 5);
        props.put("retry.backoff.ms", 5);

        KafkaProducer<String, String> myProducer = new KafkaProducer<String, String>(props);
        DateFormat dtFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss:SSS");
        String topic = "my-topic";

        int numberOfRecords = 100; // number of records to send
        long sleepTimer = 0; // how long you want to wait before the next record to be sent

        try {
                for (int i = 0; i < numberOfRecords; i++ )
                    myProducer.send(new ProducerRecord<String, String>(topic, String.format("Message: %s  sent at %s", Integer.toString(i), dtFormat.format(new Date()))));
                    Thread.sleep(sleepTimer);
                    // Thread.sleep(new Random(5000).nextLong()); // use if you want to randomize the time between record sends
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            myProducer.close();
        }

    }
}
