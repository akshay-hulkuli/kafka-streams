package com.learnkafkastreams.util;

import com.learnkafkastreams.domain.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

@Slf4j
public class OrderTimeStampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long l) {
        var order = (Order) consumerRecord.value();
        if(order != null && order.orderedDateTime() != null) {
            LocalDateTime timeStamp = order.orderedDateTime();
            log.info("timestamp in extractor : {} ", timeStamp);
            return convertToInstantFromIST(timeStamp);
        }
        return 0;
    }

    private long convertToInstantFromIST(LocalDateTime timeStamp) {
        return timeStamp.toInstant(ZoneOffset.ofHoursMinutes(5, 30)).toEpochMilli();
    }
}
