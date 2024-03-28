package com.learnkafkastreams.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafkastreams.domain.Greeting;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

@Slf4j
public class GreetingsDeserializer implements Deserializer<Greeting> {

    ObjectMapper objectMapper;

    public GreetingsDeserializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public Greeting deserialize(String s, byte[] bytes) {
        try {
            return objectMapper.readValue(bytes, Greeting.class);
        } catch (IOException e) {
            log.error("Deserialization exception : {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
