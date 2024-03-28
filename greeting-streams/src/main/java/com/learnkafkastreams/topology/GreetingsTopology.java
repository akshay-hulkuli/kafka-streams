package com.learnkafkastreams.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.List;

@Slf4j
public class GreetingsTopology {

    public static String GREETINGS_TOPIC = "greetings";
    public static String GREETINGS_UPPERCASE_TOPIC = "greetings_uppercase";
    public static String GREETINGS_SPANISH_TOPIC = "greetings_spanish";


    public static Topology buildTopology() {
        // We are building a topology here.
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // First process - Source Processor
        var greetingsStream = streamsBuilder.stream(GREETINGS_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
        var spanishGreetingsStreams = streamsBuilder.stream(GREETINGS_SPANISH_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

        var mergedStreams = greetingsStream.merge(spanishGreetingsStreams);

        greetingsStream.print(Printed.<String, String>toSysOut().withLabel("greetingsStream"));
        mergedStreams.print(Printed.<String,String>toSysOut().withLabel("mergedStream"));

        // Second process - Stream processor
        var modifiedStream = mergedStreams
                .filter((key,value) -> value.length() > 5)
                .peek((key,value) -> {
                    log.info("peeking the value after filter, value : {}", value);
                })
                .mapValues((readOnlyKeys, value) -> value.toUpperCase())
                .peek((key,value) -> {
                    log.info("peeking the value after peek, value : {}", value);
                })
//                .map((key,value) -> KeyValue.pair(key.toUpperCase(), value.toUpperCase()));
//                  .flatMap((key,value) -> {
//                      var newValues = Arrays.asList(value.split(""));
//                      return newValues.stream().map(val -> KeyValue.pair(key, val)).toList();
//                  })
                .flatMapValues((readOnlyKey, value) -> {
                    List<String> newValues = Arrays.asList(value.split(""));
                    return newValues.stream().map(String::toUpperCase).toList();
                });

        modifiedStream.print(Printed.<String, String>toSysOut().withLabel("modifiedStream"));

        // <></>hird processor - Sink processor
        modifiedStream.to(GREETINGS_UPPERCASE_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        return streamsBuilder.build();
    }

}
