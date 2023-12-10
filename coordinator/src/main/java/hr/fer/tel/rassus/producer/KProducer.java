package hr.fer.tel.rassus.producer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Scanner;

public class KProducer {

    private static String TOPIC = "KafkaTest";

    private final Producer<String, String> producer;

    public KProducer() {
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        this.producer = new org.apache.kafka.clients.producer.KafkaProducer<>(producerProperties);
    }

    public static void main(String[] args){
        KProducer kProducer = new KProducer();

        Scanner sc = new Scanner(System.in);

        while (true) {
            // insert command
            System.out.println("Write a message to send to consumer on topic " + TOPIC);
            String command = sc.nextLine();

            kProducer.sendCommandMsg(TOPIC, command);
        }
    }

    private void sendCommandMsg(String topic, String command) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, command);

        this.producer.send(record);
        this.producer.flush();
    }
}
