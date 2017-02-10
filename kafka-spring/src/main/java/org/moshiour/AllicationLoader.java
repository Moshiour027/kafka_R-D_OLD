package org.moshiour;

import org.moshiour.consumer.Receiver;
import org.moshiour.producer.Sender;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

/**
 * Created by bidro on 1/28/2017.
 */
@Component
public class AllicationLoader implements CommandLineRunner {

    @Autowired
    private Sender sender;

    @Autowired
    private Receiver receiver;


    @Override
    public void run(String... strings) throws Exception {
        sender.sendMessage("helloworld.t", "moshiur sends greeting");
        sender.sendMessage("helloworld.t", "moshiour again says hello");
        sender.sendMessage("helloworld.t", "nasrin akter again says hello");
        sender.sendMessage("helloworld.t", "meem again says hello");
        System.out.println(receiver.getLatch());
        System.out.println(receiver.getLatch().getCount());

    }
}
