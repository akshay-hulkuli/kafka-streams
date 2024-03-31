package com.learnkafkastreams.launcher;

import com.learnkafkastreams.topology.ExploreWindowTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.learnkafkastreams.topology.ExploreWindowTopology.WINDOW_WORDS;

@Slf4j
public class WindowsStreamPlaygroundApp {


    public static void main(String[] args) {

      var joinTopology = ExploreWindowTopology.build();

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "windows-2"); // consumer group
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "0");

        createTopics(config, List.of(WINDOW_WORDS ));
         var kafkaStreams = new KafkaStreams(joinTopology, config);

//       Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
//        kafkaStreams
//                .schedule(Duration.ofMinutes(1), PunctuationType.WALL_CLOCK_TIME, (timestamp) -> {
//                    // Perform any action needed on punctuate
//                    // For example, force a commit to trigger processing of the window
//                    streams.commit();
//                });


        log.info("Starting Windowed streams");
        kafkaStreams.start();
    }

    private static void createTopics(Properties config, List<String> greetings) {

        AdminClient admin = AdminClient.create(config);
        var partitions = 2;
        short replication  = 1;

        var newTopics = greetings
                .stream()
                .map(topic ->{
                    return new NewTopic(topic, partitions, replication);
                })
                .collect(Collectors.toList());

        var createTopicResult = admin.createTopics(newTopics);
        try {
            createTopicResult
                    .all().get();
            log.info("topics are created successfully");
        } catch (Exception e) {
            log.error("Exception creating topics : {} ",e.getMessage(), e);
        }
    }
}
