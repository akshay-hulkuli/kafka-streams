package com.learnkafkastreams.topology;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.*;
import java.time.format.DateTimeFormatter;

@Slf4j
public class ExploreWindowTopology {

    public static final String WINDOW_WORDS = "windows-words";

    public static Topology build() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> kstream = streamsBuilder.stream(WINDOW_WORDS, Consumed.with(Serdes.String(), Serdes.String()));

//        tumblingWindow(kstream);
        hoppingWindow(kstream);
//        slidingWindow(kstream);
        return streamsBuilder.build();
    }

    private static void tumblingWindow(KStream<String, String> kstream) {
        Duration windowSize = Duration.ofSeconds(5);
        TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(windowSize);
        KTable<Windowed<String>, Long> wordCountTable = kstream
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(timeWindows)
                .count(Named.as("tumbling-window-word-count"))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull()));

        wordCountTable
                .toStream()
                .peek((key, value) -> {
                    log.info("key: {},  value: {}", key, value);
                    printLocalDateTimes(key, value);
                })
                .print(Printed.<Windowed<String>, Long>toSysOut().withLabel("Tumbling-Windowed-Words"));
    }

    private static void hoppingWindow(KStream<String, String> kstream) {
        Duration windowSize = Duration.ofSeconds(5);
        Duration advanceSize = Duration.ofSeconds(3);
        TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(windowSize).advanceBy(advanceSize);
        KTable<Windowed<String>, Long> wordCountTable = kstream
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(timeWindows)
                .count(Named.as("hopping-window-word-count"))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull()));

        wordCountTable
                .toStream()
                .peek((key, value) -> {
                    log.info("key: {},  value: {}", key, value);
                    printLocalDateTimes(key, value);
                })
                .print(Printed.<Windowed<String>, Long>toSysOut().withLabel("Hopping-Windowed-Words"));
    }

    private static void slidingWindow(KStream<String, String> kstream) {
        Duration windowSize = Duration.ofSeconds(5);
        SlidingWindows slidingWindows = SlidingWindows.ofTimeDifferenceWithNoGrace(windowSize);
        KTable<Windowed<String>, Long> wordCountTable = kstream
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(slidingWindows)
                .count(Named.as("sliding-window-word-count"))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull()));

        wordCountTable
                .toStream()
                .peek((key, value) -> {
                    log.info("key: {},  value: {}", key, value);
                    printLocalDateTimes(key, value);
                })
                .print(Printed.<Windowed<String>, Long>toSysOut().withLabel("Sliding-Windowed-Words"));
    }


    private static void printLocalDateTimes(Windowed<String> key, Long value) {
        var startTime = key.window().startTime();
        var endTime = key.window().endTime();

        LocalDateTime startLDT = LocalDateTime.ofInstant(startTime, ZoneId.of(ZoneId.SHORT_IDS.get("IST")));
        LocalDateTime endLDT = LocalDateTime.ofInstant(endTime, ZoneId.of(ZoneId.SHORT_IDS.get("IST")));
        log.info("startLDT : {} , endLDT : {}, Count : {}", startLDT, endLDT, value);
    }

}
