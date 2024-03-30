package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.AlphabetWordAggregate;
import com.learnkafkastreams.serdes.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

@Slf4j
public class ExploreAggregateOperatorsTopology {


    public static String AGGREGATE = "aggregate";

    public static Topology build() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> inputStream = streamsBuilder.stream(AGGREGATE,
                Consumed.with(Serdes.String(), Serdes.String()));

        inputStream.print(Printed.<String, String>toSysOut().withLabel("InputStream-Aggregate"));

        KGroupedStream<String, String> groupedStream = inputStream.groupByKey(Grouped.with(Serdes.String(), Serdes.String()));

//        KGroupedStream<String, String> groupedStream = inputStream.groupBy((key,value) -> value, Grouped.with(Serdes.String(), Serdes.String()));

//        exploreCount(groupedStream);
//        exploreReduce(groupedStream);
        exploreAggregate(groupedStream);

        return streamsBuilder.build();
    }

    private static void exploreAggregate(KGroupedStream<String, String> groupedStream) {
        Initializer<AlphabetWordAggregate> alphabetWordAggregateInitializer = AlphabetWordAggregate::new;
        Aggregator<String, String, AlphabetWordAggregate> aggregator =
                (key, value, aggregate) -> aggregate.updateNewEvents(key, value);

        KTable<String, AlphabetWordAggregate> aggregateKTable = groupedStream.aggregate(alphabetWordAggregateInitializer, aggregator,
                Materialized.<String, AlphabetWordAggregate, KeyValueStore<Bytes, byte[]>>
                                as("aggreated-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(SerdesFactory.alphabetWordAggregate()));

        aggregateKTable.toStream().print(Printed.<String, AlphabetWordAggregate>toSysOut().withLabel("aggregated-KTable"));
    }

    private static void exploreReduce(KGroupedStream<String, String> groupedStream) {

        KTable<String, String> reducedTable = groupedStream.reduce((value1, value2) -> {
            log.info("value1: {} and value2: {}", value1, value2);
            return value1 + "-" + value2;
        }, Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("reduces-words")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String()));

        reducedTable.toStream().print(Printed.<String, String>toSysOut().withLabel("grouped-reduced-stream"));

    }

    private static void exploreCount(KGroupedStream<String, String> groupedStream) {
        KTable<String, Long> countByAlphabet = groupedStream
                .count(Named.as("count-per-alphabet-1"));
        countByAlphabet
                .toStream()
                .print(Printed.<String, Long>toSysOut().withLabel("words-count-per-alphabet-1"));
    }

}
