package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Greeting;
import com.learnkafkastreams.serdes.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
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

//        KStream<String, String> mergedStreams = getMergedStreamWithDefaultSerdes(streamsBuilder);
        KStream<String, Greeting> mergedStreams = getMergedStreamWithCustomSerdes(streamsBuilder);
        mergedStreams.print(Printed.<String, Greeting>toSysOut().withLabel("mergedStream"));

        // Second process - Stream processor
//        var modifiedStream = exploreOperators(mergedStreams);
        var modifiedStream = exploreErrors(mergedStreams);

        modifiedStream.print(Printed.<String, Greeting>toSysOut().withLabel("modifiedStream"));

        // <></>hird processor - Sink processor
//        modifiedStream.to(GREETINGS_UPPERCASE_TOPIC, Produced.with(Serdes.String(), SerdesFactory.greetingSerdes()));
        modifiedStream.to(GREETINGS_UPPERCASE_TOPIC, Produced.with(Serdes.String(), SerdesFactory.greetingSerdesUsingGeneric()));
//        modifiedStream.to(GREETINGS_UPPERCASE_TOPIC);

        return streamsBuilder.build();
    }

    private static KStream<String, Greeting> exploreErrors(KStream<String, Greeting> modifiedStream) {
        return modifiedStream.mapValues((readOnlyKey, value) -> {
            if(value.getMessage().equals("Transient Error")) {
                throw new IllegalArgumentException(value.getMessage());
            }
            return new Greeting(value.getMessage().toUpperCase(), value.getTimeStamp());
        });
    }

    private static KStream<String, Greeting> exploreOperators(KStream<String, Greeting> mergedStreams) {
        var modifiedStream = mergedStreams
//                .filter((key, value) -> value.length() > 5)
//                .peek((key, value) -> {
//                    log.info("peeking the value after filter, value : {}", value);
//                })
                .mapValues((readOnlyKeys, value) -> new Greeting(value.getMessage().toUpperCase(), value.getTimeStamp()));
//                .peek((key, value) -> {
//                    log.info("peeking the value after peek, value : {}", value);
//                })
//                .map((key,value) -> KeyValue.pair(key.toUpperCase(), value.toUpperCase()));
//                  .flatMap((key,value) -> {
//                      var newValues = Arrays.asList(value.split(""));
//                      return newValues.stream().map(val -> KeyValue.pair(key, val)).toList();
//                  })
//                .flatMapValues((readOnlyKey, value) -> {
//                    List<String> newValues = Arrays.asList(value.split(""));
//                    return newValues.stream().map(String::toUpperCase).toList();
//                });
        return modifiedStream;
    }

    private static KStream<String, String> getMergedStreamWithDefaultSerdes(StreamsBuilder streamsBuilder) {
        // building streams using default Serdes,
        /*
            var greetingsStream = streamsBuilder.stream(GREETINGS_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
            var spanishGreetingsStreams = streamsBuilder.stream(GREETINGS_SPANISH_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
        */

        // building streams with Default serdes
        KStream<String, String> greetingsStream = streamsBuilder.stream(GREETINGS_TOPIC);
        KStream<String, String> spanishGreetingsStreams = streamsBuilder.stream(GREETINGS_TOPIC);

        greetingsStream.print(Printed.<String, String>toSysOut().withLabel("greetingsStream"));
        return greetingsStream.merge(spanishGreetingsStreams);
    }

    private static KStream<String, Greeting> getMergedStreamWithCustomSerdes(StreamsBuilder streamsBuilder) {
        KStream<String, Greeting> greetingsStream = streamsBuilder.stream(GREETINGS_TOPIC, Consumed.with(Serdes.String(), SerdesFactory.greetingSerdes()));
        KStream<String, Greeting> spanishGreetingsStreams = streamsBuilder.stream(GREETINGS_TOPIC,
                Consumed.with(Serdes.String(), SerdesFactory.greetingSerdes()));

        greetingsStream.print(Printed.<String, Greeting>toSysOut().withLabel("greetingsStream"));
        return greetingsStream.merge(spanishGreetingsStreams);
    }

    private static KStream<String, Greeting> getMergedStreamWithCustomGenericSerdes(StreamsBuilder streamsBuilder) {
        KStream<String, Greeting> greetingsStream = streamsBuilder.stream(GREETINGS_TOPIC, Consumed.with(Serdes.String(), SerdesFactory.greetingSerdesUsingGeneric()));
        KStream<String, Greeting> spanishGreetingsStreams = streamsBuilder.stream(GREETINGS_TOPIC,
                Consumed.with(Serdes.String(), SerdesFactory.greetingSerdesUsingGeneric()));

        greetingsStream.print(Printed.<String, Greeting>toSysOut().withLabel("greetingsStream"));
        return greetingsStream.merge(spanishGreetingsStreams);
    }

}
