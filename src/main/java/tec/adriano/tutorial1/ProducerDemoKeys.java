package tec.adriano.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.stream.IntStream;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class ProducerDemoKeys {
    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
    public static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        IntStream.range(0, 10).forEach(value -> {
            String topic = "tech.adriano.kafka.test";
            String topicValue = "Hello Kafka from Java - with callbacks!";
            String key = "id_" + value;

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic, key, topicValue);

            // by providing a key, we guarantee that the same key always goes to the same partition
            logger.info("Key : {}", key);

            try {
                producer.send(record, (recordMetadata, e) -> {
                    // executes everytime a record is successfully sent
                    if (e == null) {
                        logger.info("Received new metadata: \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offsets: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);
                    }
                }).get(); //block the .send() to make it synchronous, never do this on prod!!!
            } catch (Exception e) {
                e.printStackTrace();
            }
        });


        producer.flush();
        producer.close();
    }
}
