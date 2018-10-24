package de.sw.example.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SimpleProducer
{
    public static void main(String[] args)
    {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer kafkaProducer = new KafkaProducer(properties);

        for(int i = 1; i <= 100; i++)
        {
            System.out.println(String.format("Test Nachricht - %03d", i));
            kafkaProducer.send(new ProducerRecord("Kunde1",
                    Integer.toString(i),
                    String.format("Test Nachricht - %03d", i)));
        }
        kafkaProducer.close();
    }
}
