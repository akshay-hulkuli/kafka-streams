package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.domain.Revenue;
import com.learnkafkastreams.domain.Store;
import com.learnkafkastreams.serdes.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

@Slf4j
public class OrdersTopology {
    public static final String ORDERS = "orders";
    public static final String GENERAL_ORDERS = "general_orders";
    public static final String RESTAURANT_ORDERS = "restaurant_orders";
    public static final String STORES = "stores";

    public static Topology buildTopology() {

        Predicate<String, Order> generalPredicate = (key, order) -> order.orderType().equals(OrderType.GENERAL);
        Predicate<String, Order> restaurantPredicate = (key, order) -> order.orderType().equals(OrderType.RESTAURANT);

        ValueMapper<Order, Revenue> revenueValueMapper = order -> new Revenue(order.locationId(), order.finalAmount());

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, Order> ordersKStream = streamsBuilder
                .stream(ORDERS, Consumed.with(Serdes.String(), SerdesFactory.orderSerde()));

//        KStream<String, Store> storeKStream = streamsBuilder
//                .stream(STORES, Consumed.with(Serdes.String(), SerdesFactory.storeSerde()));

        ordersKStream.print(Printed.<String, Order>toSysOut().withLabel("orderKStream"));
//        storeKStream.print(Printed.<String, Store>toSysOut().withLabel("storeKStream"));

        ordersKStream.split(Named.as("General-resturant-stream"))
                .branch(generalPredicate, Branched.withConsumer(generalOrderStream -> {
                    generalOrderStream.print(Printed.<String, Order>toSysOut().withLabel("GENERAL_ORDERS"));
                    generalOrderStream
                            .mapValues((readOnlyKey, Order) -> revenueValueMapper.apply(Order))
                            .to(GENERAL_ORDERS, Produced.with(Serdes.String(), SerdesFactory.revenueSerde()));
                }))
                .branch(restaurantPredicate, Branched.withConsumer(restaurantOrderStream -> {
                    restaurantOrderStream.print(Printed.<String, Order>toSysOut().withLabel("RESTAURANT_ORDERS"));
                    restaurantOrderStream
                            .mapValues((readOnlyKey, Order) -> revenueValueMapper.apply(Order))
                            .to(RESTAURANT_ORDERS, Produced.with(Serdes.String(), SerdesFactory.revenueSerde()));
                }));

        return streamsBuilder.build();
    }
}
