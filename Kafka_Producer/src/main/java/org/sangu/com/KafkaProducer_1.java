package org.sangu.com;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducer_1 {
    public static void main(String[] args) {
        System.out.println("program started");
        String bootstrapserver = "localhost:9092";
        String topicname = "second_topic";

        /*Setting the properties for kafka producer*/
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapserver);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        /*create the producer object*/
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        /*create a producer record*/
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicname,"1", "Atreyee");

        /*Send the record*/
        producer.send(record);

        /*flush data and close progrma*/
        producer.flush();
        producer.close();

    }
}
