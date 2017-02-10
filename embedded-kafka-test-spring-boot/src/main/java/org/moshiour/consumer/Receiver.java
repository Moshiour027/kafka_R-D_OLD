package org.moshiour.consumer;

import org.slf4j.Logger;
import org.springframework.kafka.annotation.KafkaListener;

import java.util.concurrent.CountDownLatch;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by Moshiour on 1/28/2017.
 */

public class Receiver {
    private static final Logger LOGGER = getLogger(Receiver.class);

    private CountDownLatch latch = new CountDownLatch(1);

    @KafkaListener(topics = "helloworld.t")
    public void receiveMessage(String message) {
        LOGGER.info("received message='{}'", message);
        latch.countDown();
    }

    public CountDownLatch getLatch() {
        return latch;
    }

}
