package com.learnkafkastreams.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

public class ExploreKTableTopology {

    public static String WORDS = "words";

    public static Topology build() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KTable<String, String> wordsKTable = streamsBuilder.table(
                WORDS, Consumed.with(Serdes.String(), Serdes.String()), Materialized.as("words-store"));

        wordsKTable
                .filter((key, value) -> value.length() > 2)
                .mapValues((key, value) -> value.toUpperCase())
                .toStream().print(Printed.<String, String>toSysOut().withLabel("WordsKTable"));

        GlobalKTable<String, String> wordsGlobalKTable = streamsBuilder.globalTable(
                WORDS, Consumed.with(Serdes.String(), Serdes.String()), Materialized.as("words-global-store"));

//        wordsGlobalKTable.


        return streamsBuilder.build();
    }

}
