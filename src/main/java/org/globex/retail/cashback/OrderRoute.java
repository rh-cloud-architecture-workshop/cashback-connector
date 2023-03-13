package org.globex.retail.cashback;

import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;

public class OrderRoute extends RouteBuilder {

    @Override
    public void configure() throws Exception {
        from("kafka:{{kafka.order.topic.name}}?groupId={{kafka.order.consumer.group}}" +
                "&autoOffsetReset=earliest")
                .routeId("kafka2CashbackService")
                .log(LoggingLevel.INFO, "Order event received: ${body}")
                .setHeader(Exchange.HTTP_METHOD, constant("POST"))
                .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
                .to("{{cashback-service.url}}");
    }
}
