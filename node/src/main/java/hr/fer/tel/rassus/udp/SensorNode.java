package hr.fer.tel.rassus.udp;

import hr.fer.tel.rassus.udp.client.UDPClient;
import hr.fer.tel.rassus.udp.server.UDPServer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class SensorNode {
    /*
    private final UDPServer server;
    private final UDPClient client;

    public SensorNode(UDPServer server, UDPClient client) {
        this.server = server;
        this.client = client;
    }
    */
    private static final String REGISTER_TOPIC = "Register";
    private static final String COMMAND_TOPIC = "Command";

    private static final String START_MSG = "Start";
    private static final String STOP_MSG = "Stop";

    private final Consumer<String, JSONObject> consumer;
    private final Producer<String, JSONObject> producer;
    
    private static Map<Integer, JSONObject> sensors = new HashMap<>();


    public SensorNode() {
        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());

        this.consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(consumerProperties);

        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        this.producer = new org.apache.kafka.clients.producer.KafkaProducer<>(producerProperties);
    }


    public static void main(String[] args) throws IOException {
        // inicijalizacija čvora
        Scanner sc = new Scanner(System.in);
        System.out.println("Insert id (integer): ");
        int id = sc.nextInt();
        System.out.println("Insert port (integer): ");
        int port = sc.nextInt();
        sc.close();

        UDPServer server = new UDPServer(id, "localhost", port);
        UDPClient client = new UDPClient();
        //String message = "Message";
        //String response = client.sendMessage(message, 8000);

        SensorNode sensorNode = new SensorNode();
        // TODO pretplata na temu Register i Command
        sensorNode.consumer.subscribe(Collections.singleton(REGISTER_TOPIC));
        sensorNode.consumer.subscribe(Collections.singleton(COMMAND_TOPIC));

        // TODO čekanje na kontrolnu poruku Start od koordinatora
        System.out.println("Waiting for message Start to arrive on topic " + COMMAND_TOPIC);
        AtomicBoolean start = new AtomicBoolean(false);
        while (!start.get()) {
            ConsumerRecords<String, JSONObject> consumerRecords = sensorNode.consumer.poll(Duration.ofMillis(1000));
            consumerRecords.forEach(record -> {
                System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());
                if (record.value().equals(START_MSG)) {
                    start.set(true);
                }
            });

            sensorNode.consumer.commitAsync();
        }

        // TODO slanje registracijske poruke na temu Register u JSON formatu (id, address, port)
        JSONObject registrationData = new JSONObject();
        registrationData.put("id", server.getId());
        registrationData.put("address", server.getAddress());
        registrationData.put("port", server.getPort());
        ProducerRecord<String, JSONObject> registrationRecord = new ProducerRecord<>(REGISTER_TOPIC, REGISTER_TOPIC, registrationData);
        sensorNode.producer.send(registrationRecord);
        sensorNode.producer.flush();

        // TODO primanje reg poruka od drugih čvorova putem teme Register
        AtomicReference<ConsumerRecord<String, JSONObject>> unprocessedRecord = null;
        AtomicBoolean register = new AtomicBoolean(false);
        while (!register.get()) {
            ConsumerRecords<String, JSONObject> consumerRecords = sensorNode.consumer.poll(Duration.ofMillis(1000));
            consumerRecords.forEach(record -> {
                System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());
                if (!record.key().equals(REGISTER_TOPIC)) {
                    unprocessedRecord.set(record);
                    register.set(true);
                } else {
                    sensors.put(record.value().getInt("id"), record.value());
                }
            });

            sensorNode.consumer.commitAsync();
        }

        // TODO komunikacija sa ostalim čvorovima (UDP)
        //SensorNode sensorNode = new SensorNode(server, client);
        server.receive();
    }
    
}
