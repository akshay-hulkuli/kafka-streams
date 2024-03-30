package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.*;
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
public class OrdersTopology {
    public static final String ORDERS = "orders";
    public static final String GENERAL_ORDERS = "general_orders";
    public static final String GENERAL_ORDERS_COUNT = "general_orders_count";
    public static final String GENERAL_ORDERS_REVENUE = "general_orders_revenue";
    public static final String RESTAURANT_ORDERS = "restaurant_orders";
    public static final String RESTAURANT_ORDERS_COUNT = "restaurant_orders_count";
    public static final String RESTAURANT_ORDERS_REVENUE = "restaurant_orders_revenue";
    public static final String STORES = "stores";

    public static Topology buildTopology() {

        Predicate<String, Order> generalPredicate = (key, order) -> order.orderType().equals(OrderType.GENERAL);
        Predicate<String, Order> restaurantPredicate = (key, order) -> order.orderType().equals(OrderType.RESTAURANT);

        ValueMapper<Order, Revenue> revenueValueMapper = order -> new Revenue(order.locationId(), order.finalAmount());

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, Order> ordersKStream = streamsBuilder
                .stream(ORDERS, Consumed.with(Serdes.String(), SerdesFactory.orderSerde())).selectKey((key, value) -> value.locationId());

//        KStream<String, Store> storeKStream = streamsBuilder
//                .stream(STORES, Consumed.with(Serdes.String(), SerdesFactory.storeSerde()));

        ordersKStream.print(Printed.<String, Order>toSysOut().withLabel("orderKStream"));
//        storeKStream.print(Printed.<String, Store>toSysOut().withLabel("storeKStream"));

        ordersKStream.split(Named.as("General-resturant-stream"))
                .branch(generalPredicate, Branched.withConsumer(generalOrderStream -> {
                    generalOrderStream.print(Printed.<String, Order>toSysOut().withLabel("GENERAL_ORDERS"));
//                    generalOrderStream
//                            .mapValues((readOnlyKey, Order) -> revenueValueMapper.apply(Order))
//                            .to(GENERAL_ORDERS, Produced.with(Serdes.String(), SerdesFactory.revenueSerde()));

                    aggregateOrdersByCount(generalOrderStream, GENERAL_ORDERS_COUNT);
                    aggregateOrderByRevenue(generalOrderStream, GENERAL_ORDERS_REVENUE);

                }))
                .branch(restaurantPredicate, Branched.withConsumer(restaurantOrderStream -> {
                    restaurantOrderStream.print(Printed.<String, Order>toSysOut().withLabel("RESTAURANT_ORDERS"));
//                    restaurantOrderStream
//                            .mapValues((readOnlyKey, Order) -> revenueValueMapper.apply(Order))
//                            .to(RESTAURANT_ORDERS, Produced.with(Serdes.String(), SerdesFactory.revenueSerde()));

                    aggregateOrdersByCount(restaurantOrderStream, RESTAURANT_ORDERS_COUNT);
                    aggregateOrderByRevenue(restaurantOrderStream, RESTAURANT_ORDERS_REVENUE);

                }));

        return streamsBuilder.build();
    }

    private static void aggregateOrderByRevenue(KStream<String, Order> generalOrderStream, String storeName) {

        Initializer<TotalRevenue> initializer = TotalRevenue::new;
        Aggregator<String, Order, TotalRevenue> aggregator =
                (key, value, aggregate) -> aggregate.updateAggregate(key, value);

        KTable<String, TotalRevenue> revenueKTable = generalOrderStream
                .groupBy((key, value) -> value.locationId(), Grouped.with(Serdes.String(), SerdesFactory.orderSerde()))
                .aggregate(
                        initializer,
                        aggregator,
                        Materialized.<String, TotalRevenue, KeyValueStore<Bytes, byte[]>>
                                        as(storeName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SerdesFactory.totalRevenueSerde()));

        revenueKTable.toStream().print(Printed.<String, TotalRevenue>toSysOut().withLabel(storeName));
    }

    private static void aggregateOrdersByCount(KStream<String, Order> generalOrderStream, String storeName) {
        KTable<String, Long> kTable = generalOrderStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerde()))
                .count(Named.as(storeName), Materialized.as(storeName));

        kTable.toStream().print(Printed.<String, Long>toSysOut().withLabel(storeName));

    }
}
