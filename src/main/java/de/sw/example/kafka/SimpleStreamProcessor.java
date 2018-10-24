package de.sw.example.kafka;

import joptsimple.util.KeyValuePair;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
//import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.state.KeyValueStore;


import java.util.Arrays;
import java.util.Properties;


public class SimpleStreamProcessor
{
    public static void main(String[] args)
    {
        Properties KafkaStreamProperties = new Properties();
        KafkaStreamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "SimpleStreamProcessor");
        KafkaStreamProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        KafkaStreamProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        KafkaStreamProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());


        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> source = streamsBuilder.stream("Kunde1");

        source.filter((key, value) -> (Long.valueOf(key) % 5) == 0)
                .map((key, value) -> KeyValue.pair("NeuerSchlüssel-"+ key, "NeueNachricht-" + value))
                .to("Kunde2");

        Topology topology = streamsBuilder.build();
        System.out.println(topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, KafkaStreamProperties);
        streams.start();
    }
}
