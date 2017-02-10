package org.moshiour;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.moshiour.consumer.Receiver;
import org.moshiour.producer.Sender;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
public class KafkaSpringApplicationTests {

	@Test
	public void contextLoads() {
	}


	@Autowired
	private Sender sender;

	@Autowired
	private Receiver receiver;

	@Test
	public void testReceiver() throws Exception {
		sender.sendMessage("helloworld.t", "Hello Spring Kafka!");
		//receiver.getLatch().await(2000, TimeUnit.MILLISECONDS);
		assertThat(receiver.getLatch().getCount()).isEqualTo(1);
	}

}
