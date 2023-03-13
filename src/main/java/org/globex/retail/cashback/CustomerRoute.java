package org.globex.retail.cashback;

import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;

public class CustomerRoute extends RouteBuilder {


    @Override
    public void configure() throws Exception {
        from("kafka:{{kafka.customer.topic.name}}?groupId={{kafka.customer.consumer.group}}" +
                "&autoOffsetReset=earliest")
                .routeId("kafka2PostgresqlCustomer")
                .transacted()
                .unmarshal().json()
                .log(LoggingLevel.DEBUG, "Customer event received: ${body}")
                .filter().jsonpath("$..[?(@.op =~ /d|t|m/)]")
                .log(LoggingLevel.DEBUG, "Filtering out change event which is not 'create', 'read' or or 'update'")
                .stop()
                .end()
                .setHeader("customerId", simple("${body[after][user_id]}"))
                .setHeader("customerName", simple("${body[after][first_name]} ${body[after][last_name]}"))
                .choice()
                .when().jsonpath("$..[?(@.op =~ /c|r/)]")
                    .log(LoggingLevel.INFO, "Create customer ${header.customerId}")
                    .setBody(constant("INSERT INTO customer (customer_id, name) VALUES (:?customerId, :?customerName);"))
                .when().jsonpath("$..[?(@.op == 'u')]")
                    .log(LoggingLevel.INFO, "Update customer ${header.customerId}")
                    .setBody(constant("UPDATE customer SET name = :?customerName WHERE customer_id = :?customerId;"))
                .end()
                .log(LoggingLevel.DEBUG, "SQL statement: ${body}")
                .to("jdbc:cashback?useHeadersAsParameters=true&resetAutoCommit=false");
    }
}
