package tshepo;

import org.apache.kafka.clients.producer.*;
import java.util.Properties;

public class UserProfileProducer {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        String[] names = {"Alice", "Bob", "Charlie", "Daisy", "Ethan", "Fiona", "George", "Hannah", "Ivan", "Jade"};
        int updatesPerUser = 100;

        for (int i = 0; i < updatesPerUser; i++) {
            for (int j = 0; j < names.length; j++) {
                String key = names[j]; // user name as key
                String value = names[j] + "_v" + i; // versioned name as value
                ProducerRecord<String, String> record = new ProducerRecord<>("user-profiles", key, value);
                producer.send(record);
                Thread.sleep(5); // Slight delay for readability
            }
        }

        producer.flush();
        producer.close();
        System.out.println("✔ Sent " + (names.length * updatesPerUser) + " profile updates.");
    }
}
