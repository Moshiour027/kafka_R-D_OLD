package org.moshiour.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

/**
 * Created by Moshiour on 1/28/2017.
 */
public class Sender {
    private static final Logger LOGGER = LoggerFactory.getLogger(Sender.class);

    @Autowired
    private KafkaTemplate<Integer, String> kafkaTemplate;



    public void sendMessage(String topic, String message) {
        //the KafkaTemplate provides asynchronouse send method
        //returning  a Future

        //you can register a callback with the listener to receive the result of the
        //send asynchronously

        ListenableFuture<SendResult<Integer, String>> future = kafkaTemplate.send(topic, message);
        future.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                LOGGER.info("send message='{}' with offset={}", message,
                        result.getRecordMetadata().offset());
                System.out.println(message);

            }

            @Override
            public void onFailure(Throwable ex) {
                LOGGER.error("unable to send message='{}'", message, ex);
            }



        });
    }
}
