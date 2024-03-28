package com.learnkafkastreams.serdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;

@Slf4j
public class JsonSerializer<T> implements Serializer<T> {

    private final ObjectMapper objectMapper =
            new ObjectMapper().registerModule(new JavaTimeModule())
                    .configure(SerializationFeature.WRITE_DATE_KEYS_AS_TIMESTAMPS, false);
    @Override
    public byte[] serialize(String s, T obj) {
        try {
            return objectMapper.writeValueAsBytes(obj);
        } catch (JsonProcessingException e) {
            log.error("Exception in the serializer : {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
