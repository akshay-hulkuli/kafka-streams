package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.*;
import com.learnkafkastreams.serdes.SerdesFactory;
import com.learnkafkastreams.util.OrderTimeStampExtractor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;

@Slf4j
public class OrdersTopology {
    public static final String ORDERS = "orders";
    public static final String GENERAL_ORDERS = "general_orders";
    public static final String GENERAL_ORDERS_COUNT = "general_orders_count";
    public static final String GENERAL_ORDERS_WINDOWS = "general_orders_windows";
    public static final String GENERAL_ORDERS_REVENUE = "general_orders_revenue";
    public static final String GENERAL_ORDERS_REVENUE_BY_WINDOWS = "general_orders_revenue_by_windows";
    public static final String RESTAURANT_ORDERS = "restaurant_orders";
    public static final String RESTAURANT_ORDERS_COUNT = "restaurant_orders_count";
    public static final String RESTAURANT_ORDERS_WINDOWS = "restaurant_orders_windows";
    public static final String RESTAURANT_ORDERS_REVENUE = "restaurant_orders_revenue";
    public static final String RESTAURANT_ORDERS_REVENUE_BY_WINDOW = "restaurant_orders_revenue_by_window";
    public static final String STORES = "stores";

    public static Topology buildTopology() {

        Predicate<String, Order> generalPredicate = (key, order) -> order.orderType().equals(OrderType.GENERAL);
        Predicate<String, Order> restaurantPredicate = (key, order) -> order.orderType().equals(OrderType.RESTAURANT);

        ValueMapper<Order, Revenue> revenueValueMapper = order -> new Revenue(order.locationId(), order.finalAmount());

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, Order> ordersKStream = streamsBuilder
                .stream(
                        ORDERS,
                        Consumed.with(Serdes.String(), SerdesFactory.orderSerde())
                                .withTimestampExtractor(new OrderTimeStampExtractor())
                )
                .selectKey((key, value) -> value.locationId());

        KTable<String, Store> storeKTable = streamsBuilder
                .table(STORES, Consumed.with(Serdes.String(), SerdesFactory.storeSerde()), Materialized.as("store-table"));

        ordersKStream.print(Printed.<String, Order>toSysOut().withLabel("orderKStream"));
        storeKTable.toStream().print(Printed.<String, Store>toSysOut().withLabel("storeKTable"));

        ordersKStream.split(Named.as("General-resturant-stream"))
                .branch(generalPredicate, Branched.withConsumer(generalOrderStream -> {
                    generalOrderStream.print(Printed.<String, Order>toSysOut().withLabel("GENERAL_ORDERS"));
//                    generalOrderStream
//                            .mapValues((readOnlyKey, Order) -> revenueValueMapper.apply(Order))
//                            .to(GENERAL_ORDERS, Produced.with(Serdes.String(), SerdesFactory.revenueSerde()));

//                    aggregateOrdersByCount(generalOrderStream, GENERAL_ORDERS_COUNT, storeKTable);
                    aggregateOrdersCountByTimeWindows(generalOrderStream, GENERAL_ORDERS_WINDOWS, storeKTable);
//                    aggregateOrderByRevenue(generalOrderStream, GENERAL_ORDERS_REVENUE, storeKTable);
                    aggregateOrderRevenueByTimeWindows(generalOrderStream, GENERAL_ORDERS_REVENUE_BY_WINDOWS, storeKTable);

                }))
                .branch(restaurantPredicate, Branched.withConsumer(restaurantOrderStream -> {
                    restaurantOrderStream.print(Printed.<String, Order>toSysOut().withLabel("RESTAURANT_ORDERS"));
//                    restaurantOrderStream
//                            .mapValues((readOnlyKey, Order) -> revenueValueMapper.apply(Order))
//                            .to(RESTAURANT_ORDERS, Produced.with(Serdes.String(), SerdesFactory.revenueSerde()));

//                    aggregateOrdersByCount(restaurantOrderStream, RESTAURANT_ORDERS_COUNT, storeKTable);
                    aggregateOrdersCountByTimeWindows(restaurantOrderStream, RESTAURANT_ORDERS_WINDOWS, storeKTable);
//                    aggregateOrderByRevenue(restaurantOrderStream, RESTAURANT_ORDERS_REVENUE, storeKTable);
                    aggregateOrderRevenueByTimeWindows(restaurantOrderStream, RESTAURANT_ORDERS_REVENUE_BY_WINDOW, storeKTable);
                }));

        return streamsBuilder.build();
    }

    private static void aggregateOrderRevenueByTimeWindows(KStream<String, Order> generalOrderStream, String storeName,
                                                           KTable<String, Store> storeKTable) {
        Initializer<TotalRevenue> initializer = TotalRevenue::new;
        Aggregator<String, Order, TotalRevenue> aggregator =
                (key, value, aggregate) -> aggregate.updateAggregate(key, value);

        Duration windowSize = Duration.ofSeconds(15);
        TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(windowSize);

        KTable<Windowed<String>, TotalRevenue> revenueKTable = generalOrderStream
                .groupBy((key, value) -> value.locationId(), Grouped.with(Serdes.String(), SerdesFactory.orderSerde()))
                .windowedBy(timeWindows)
                .aggregate(
                        initializer,
                        aggregator,
                        Materialized.<String, TotalRevenue, WindowStore<Bytes, byte[]>>
                                        as(storeName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SerdesFactory.totalRevenueSerde()));

        revenueKTable
                .toStream()
                .peek((key, value) -> log.info("{} --> Key : {}, Value : {}", storeName, key, value))
                .print(Printed.<Windowed<String>, TotalRevenue>toSysOut().withLabel(storeName));

        // KTable to KTable join

        ValueJoiner<TotalRevenue, Store, TotalRevenueWithAddress> valueJoiner = TotalRevenueWithAddress::new;
        var joinedParams = Joined.with(Serdes.String(), SerdesFactory.totalRevenueSerde(), SerdesFactory.storeSerde());

        KStream<String, TotalRevenueWithAddress> joinedKTable =
                revenueKTable
                        .toStream()
                        .map((key, value) -> KeyValue.pair(key.key(), value))
                        .join(storeKTable, valueJoiner, joinedParams);
        joinedKTable.print(Printed.<String, TotalRevenueWithAddress>toSysOut().withLabel("Joined-KTable"));
    }

    private static void aggregateOrdersCountByTimeWindows(KStream<String, Order> generalOrderStream, String storeName, KTable<String, Store> storeKTable) {

        Duration windowSize = Duration.ofSeconds(15);
        TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(windowSize);

        KTable<Windowed<String>, Long> kTable = generalOrderStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerde()))
                .windowedBy(timeWindows)
                .count(Named.as(storeName), Materialized.as(storeName))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull()));

        kTable.toStream().print(Printed.<Windowed<String>, Long>toSysOut().withLabel(storeName));

        ValueJoiner<Long, Store, TotalCountWithAddress> valueJoiner = TotalCountWithAddress::new;

        var joinedParams = Joined.with(Serdes.String(), Serdes.Long(), SerdesFactory.storeSerde());

        KStream<String, TotalCountWithAddress> joinedKTable =
                kTable
                        .toStream()
                        .map((key, value) -> KeyValue.pair(key.key(), value))
                        .join(storeKTable, valueJoiner, joinedParams);
        joinedKTable.print(Printed.<String, TotalCountWithAddress>toSysOut().withLabel("Joined-KTable-count"));
    }

    private static void aggregateOrderByRevenue(KStream<String, Order> generalOrderStream, String storeName,
                                                KTable<String, Store> storeKTable) {

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

        // KTable to KTable join

        ValueJoiner<TotalRevenue, Store, TotalRevenueWithAddress> valueJoiner = TotalRevenueWithAddress::new;

        KTable<String, TotalRevenueWithAddress> joinedKTable = revenueKTable.join(storeKTable, valueJoiner);
        joinedKTable.toStream().print(Printed.<String, TotalRevenueWithAddress>toSysOut().withLabel("Joined-KTable"));
    }

    private static void aggregateOrdersByCount(KStream<String, Order> generalOrderStream, String storeName,
                                               KTable<String, Store> storeKTable) {
        KTable<String, Long> kTable = generalOrderStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerde()))
                .count(Named.as(storeName), Materialized.as(storeName));

        kTable.toStream().print(Printed.<String, Long>toSysOut().withLabel(storeName));

        ValueJoiner<Long, Store, TotalCountWithAddress> valueJoiner = TotalCountWithAddress::new;

        KTable<String, TotalCountWithAddress> joinedKTable = kTable.join(storeKTable, valueJoiner);
        joinedKTable.toStream().print(Printed.<String, TotalCountWithAddress>toSysOut().withLabel("Joined-KTable-count"));

    }
}
