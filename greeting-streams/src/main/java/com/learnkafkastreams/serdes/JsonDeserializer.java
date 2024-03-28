package com.learnkafkastreams.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

@Slf4j
public class JsonDeserializer<T> implements Deserializer<T> {

    private final Class<T> destination;
    private final ObjectMapper objectMapper =
            new ObjectMapper().registerModule(new JavaTimeModule())
                    .configure(SerializationFeature.WRITE_DATE_KEYS_AS_TIMESTAMPS, false);

    public JsonDeserializer(Class<T> destination) {
        this.destination = destination;
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        try {
            return objectMapper.readValue(bytes, destination);
        } catch (IOException e) {
            log.error("Exception in deserializer : {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
