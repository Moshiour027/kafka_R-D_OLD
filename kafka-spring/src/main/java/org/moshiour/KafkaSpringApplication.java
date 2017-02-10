package org.moshiour;

import org.moshiour.producer.Sender;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaSpringApplication {



	public static void main(String[] args) {
		SpringApplication.run(KafkaSpringApplication.class, args);



	}
}
