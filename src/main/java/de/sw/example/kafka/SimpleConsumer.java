package de.sw.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class SimpleConsumer
{
    public static void main(String[] args)
    {
        String topic = "Kunde1";
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "Kunde1_Gruppe");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Arrays.asList(topic));

        System.out.println(String.format("Konsumiere Daten aus Topic: '%s'", topic));

        boolean running = true;
        int count = 0;
        try
        {
            while(running)
            {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(100);

                for (ConsumerRecord<String, String> record : records)
                {
                    System.out.println(String.format("Partition = '%s' - Offset = '%s' " +
                                    "- Schluessel = '%s' - Wert = '%s'",
                            record.partition(),
                            record.offset(),
                            record.key(),
                            record.value()));
                }
                System.out.println("Verarbeitung des Stapels beendet. Kurze Pause...");
                Thread.sleep(1000);
            }
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        finally
        {
            kafkaConsumer.close();
        }
    }
}