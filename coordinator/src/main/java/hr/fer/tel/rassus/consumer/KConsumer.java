package hr.fer.tel.rassus.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class KConsumer {
    private static String TOPIC = "KafkaTest";

    private Consumer<String, String> consumer;

    public KConsumer() {
        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        //consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "Sensors"); // poruka se dostavlja samo jednom potrosacu iz grupe
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        //consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // potrošač automatski potvrđiva offset nakon svake uspješne obrade poruka
        //consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        this.consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(consumerProperties);
    }

    public static void main(String[] args){
        KConsumer kConsumer = new KConsumer();

        kConsumer.consumer.subscribe(Collections.singleton(TOPIC));

        System.out.println("Waiting for messaged to arrive on topic " + TOPIC);

        while (true) {
            ConsumerRecords<String, String> consumerRecords = kConsumer.consumer.poll(Duration.ofMillis(1000));

            consumerRecords.forEach(record -> {
                System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());
            });

            kConsumer.consumer.commitAsync();
        }
    }
}
