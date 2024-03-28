package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.domain.Store;
import com.learnkafkastreams.serdes.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;

@Slf4j
public class OrdersTopology {
    public static final String ORDERS = "orders";
    public static final String STORES = "stores";

    public static Topology buildTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, Order> ordersKStream = streamsBuilder
                .stream(ORDERS, Consumed.with(Serdes.String(), SerdesFactory.orderSerde()));

        KStream<String, Store> storeKStream = streamsBuilder
                .stream(STORES, Consumed.with(Serdes.String(), SerdesFactory.storeSerde()));

        ordersKStream.print(Printed.<String, Order>toSysOut().withLabel("orderKStream"));
        storeKStream.print(Printed.<String, Store>toSysOut().withLabel("storeKStream"));

        return streamsBuilder.build();
    }
}
