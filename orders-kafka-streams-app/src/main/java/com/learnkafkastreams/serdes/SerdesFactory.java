package com.learnkafkastreams.serdes;

import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.domain.Revenue;
import com.learnkafkastreams.domain.Store;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class SerdesFactory {

    public static Serde<Order> orderSerde() {
        JsonSerializer<Order> jsonSerializer = new JsonSerializer<>();
        JsonDeserializer<Order> jsonDeserializer = new JsonDeserializer<>(Order.class);

        return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
    }

    public static Serde<Revenue> revenueSerde() {
        JsonSerializer<Revenue> jsonSerializer = new JsonSerializer<>();
        JsonDeserializer<Revenue> jsonDeserializer = new JsonDeserializer<>(Revenue.class);

        return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
    }

    public static Serde<Store> storeSerde() {
        JsonSerializer<Store> jsonSerializer = new JsonSerializer<>();
        JsonDeserializer<Store> jsonDeserializer = new JsonDeserializer<>(Store.class);

        return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
    }
}
