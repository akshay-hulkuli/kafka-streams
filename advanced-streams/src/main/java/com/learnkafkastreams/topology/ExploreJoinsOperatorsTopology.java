package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Alphabet;
import com.learnkafkastreams.serdes.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;

@Slf4j
public class ExploreJoinsOperatorsTopology {


    public static String ALPHABETS = "alphabets"; // A => First letter in the english alphabet
    public static String ALPHABETS_ABBREVIATIONS = "alphabets_abbreviations"; // A=> Apple


    public static Topology build() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

//        joinKStreamWithKTable(streamsBuilder);
//        joinKTableWithKTable(streamsBuilder);
//        joinKStreamWithKStream(streamsBuilder);
//        leftJoinKStreamWithKStream(streamsBuilder);
        outerJoinKStreamWithKStream(streamsBuilder);
        return streamsBuilder.build();
    }

    private static void joinKStreamWithKStream(StreamsBuilder streamsBuilder) {
        KStream<String, String> abbreviationStream = streamsBuilder
                .stream(ALPHABETS_ABBREVIATIONS, Consumed.with(Serdes.String(), Serdes.String()));
        abbreviationStream.print(Printed.<String, String>toSysOut().withLabel(ALPHABETS_ABBREVIATIONS));
        KStream<String, String> alphabetStream = streamsBuilder
                .stream(ALPHABETS, Consumed.with(Serdes.String(), Serdes.String()));
        alphabetStream.print(Printed.<String, String>toSysOut().withLabel(ALPHABETS));

        ValueJoiner<String, String, Alphabet> valueJoiner = Alphabet::new;

        var fiveSecWindow = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5));

        StreamJoined<String, String, String> joinedParams =
                StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
                        .withName("alphabets-join")
                        .withStoreName("alphabets-join");

        KStream<String, Alphabet> joinedStream = abbreviationStream
                .join(alphabetStream, valueJoiner, fiveSecWindow, joinedParams);

        joinedStream.print(Printed.<String, Alphabet>toSysOut().withLabel("Joined-Stream"));

    }

    private static void joinKStreamWithKTable(StreamsBuilder streamsBuilder) {
        KStream<String, String> abbreviationsStream = streamsBuilder
                .stream(ALPHABETS_ABBREVIATIONS, Consumed.with(Serdes.String(), Serdes.String()));

        abbreviationsStream.print(Printed.<String, String>toSysOut().withLabel(ALPHABETS_ABBREVIATIONS));

        KTable<String, String> alphabetsTable = streamsBuilder
                .table(ALPHABETS,
                        Consumed.with(Serdes.String(), Serdes.String()),
                        Materialized.as("alphabet-store"));

        alphabetsTable.toStream().print(Printed.<String, String>toSysOut().withLabel(ALPHABETS));

        ValueJoiner<String, String, Alphabet> valueJoiner = Alphabet::new;

        KStream<String, Alphabet> joinedStream = abbreviationsStream.join(alphabetsTable, valueJoiner);

        joinedStream.print(Printed.<String, Alphabet>toSysOut().withLabel("Joined-Stream"));


    }

    private static void joinKStreamWithGlobalKTable(StreamsBuilder streamsBuilder) {
        KStream<String, String> abbreviationsStream = streamsBuilder
                .stream(ALPHABETS_ABBREVIATIONS, Consumed.with(Serdes.String(), Serdes.String()));

        abbreviationsStream.print(Printed.<String, String>toSysOut().withLabel(ALPHABETS_ABBREVIATIONS));

        GlobalKTable<String, String> alphabetsTable = streamsBuilder
                .globalTable(ALPHABETS,
                        Consumed.with(Serdes.String(), Serdes.String()),
                        Materialized.as("global-alphabet-store"));

//        not available ->
//        alphabetsTable.toStream().print(Printed.<String, String>toSysOut().withLabel(ALPHABETS));

        ValueJoiner<String, String, Alphabet> valueJoiner = Alphabet::new;

        KeyValueMapper<String, String, String> keyValueMapper = (leftKey, rightKey) -> leftKey;

        KStream<String, Alphabet> joinedStream = abbreviationsStream.join(alphabetsTable, keyValueMapper, valueJoiner);

        joinedStream.print(Printed.<String, Alphabet>toSysOut().withLabel("Joined-Stream"));


    }

    private static void joinKTableWithKTable(StreamsBuilder streamsBuilder) {
        KTable<String, String> abbreviationsStream = streamsBuilder
                .table(ALPHABETS_ABBREVIATIONS,
                        Consumed.with(Serdes.String(), Serdes.String()),
                        Materialized.as("abbreviation-table"));

        abbreviationsStream.toStream().print(Printed.<String, String>toSysOut().withLabel(ALPHABETS_ABBREVIATIONS));

        KTable<String, String> alphabetsTable = streamsBuilder
                .table(ALPHABETS,
                        Consumed.with(Serdes.String(), Serdes.String()),
                        Materialized.as("alphabet-store"));

        alphabetsTable.toStream().print(Printed.<String, String>toSysOut().withLabel(ALPHABETS));

        ValueJoiner<String, String, Alphabet> valueJoiner = Alphabet::new;


        KTable<String, Alphabet> joinedTable = abbreviationsStream.join(alphabetsTable, valueJoiner);

        joinedTable.toStream().print(Printed.<String, Alphabet>toSysOut().withLabel("Joined-Stream"));


    }

    private static void leftJoinKStreamWithKStream(StreamsBuilder streamsBuilder) {
        KStream<String, String> abbreviationStream = streamsBuilder
                .stream(ALPHABETS_ABBREVIATIONS, Consumed.with(Serdes.String(), Serdes.String()));
        abbreviationStream.print(Printed.<String, String>toSysOut().withLabel(ALPHABETS_ABBREVIATIONS));
        KStream<String, String> alphabetStream = streamsBuilder
                .stream(ALPHABETS, Consumed.with(Serdes.String(), Serdes.String()));
        alphabetStream.print(Printed.<String, String>toSysOut().withLabel(ALPHABETS));

        ValueJoiner<String, String, Alphabet> valueJoiner = Alphabet::new;

        var fiveSecWindow = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5));

        StreamJoined<String, String, String> joinedParams =
                StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String());

        KStream<String, Alphabet> joinedStream = abbreviationStream
                .leftJoin(alphabetStream, valueJoiner, fiveSecWindow, joinedParams);

        joinedStream.print(Printed.<String, Alphabet>toSysOut().withLabel("Joined-Stream"));

    }

    private static void outerJoinKStreamWithKStream(StreamsBuilder streamsBuilder) {
        KStream<String, String> abbreviationStream = streamsBuilder
                .stream(ALPHABETS_ABBREVIATIONS, Consumed.with(Serdes.String(), Serdes.String()));
        abbreviationStream.print(Printed.<String, String>toSysOut().withLabel(ALPHABETS_ABBREVIATIONS));
        KStream<String, String> alphabetStream = streamsBuilder
                .stream(ALPHABETS, Consumed.with(Serdes.String(), Serdes.String()));
        alphabetStream.print(Printed.<String, String>toSysOut().withLabel(ALPHABETS));

        ValueJoiner<String, String, Alphabet> valueJoiner = Alphabet::new;

        var fiveSecWindow = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5));

        StreamJoined<String, String, String> joinedParams =
                StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String());

        KStream<String, Alphabet> joinedStream = abbreviationStream
                .outerJoin(alphabetStream, valueJoiner, fiveSecWindow, joinedParams);

        joinedStream.print(Printed.<String, Alphabet>toSysOut().withLabel("Joined-Stream"));

    }

}
