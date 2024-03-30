package com.learnkafkastreams.domain;

import java.math.BigDecimal;

public record TotalRevenue(String locationId,
                           Integer runnuingOrderCount,
                           BigDecimal runningRevenue) {
    public TotalRevenue() {
        this("", 0, BigDecimal.ZERO);
    }

    public TotalRevenue updateAggregate(String key, Order value) {
        var newOrdersCount = this.runnuingOrderCount + 1;
        var newRevenue = this.runningRevenue.add(value.finalAmount());
        return new TotalRevenue(key, newOrdersCount, newRevenue);
    }
}
