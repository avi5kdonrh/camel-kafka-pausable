package org.acme;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@Component
public class KafkaRoute extends RouteBuilder {
    AtomicBoolean atomicBoolean = new AtomicBoolean(true);
    AtomicReference<Instant> atomicReference = new AtomicReference<>();

    @Override
    public void configure() throws Exception {
        from("kafka:my-topic?autoCommitEnable=false&allowManualCommit=true&groupId=gr1")
                .pausable(new MySeekPolicy(), o -> {
                    if (atomicReference.get() != null && atomicReference.get().isBefore(Instant.now())) {
                        atomicBoolean.set(true);
                    }
                    return atomicBoolean.get();
                })
                .to("log:logx?showAll=true")
                .process(exchange -> {
                    Long time = exchange.getIn().getHeader(KafkaConstants.TIMESTAMP, Long.class);
                    Instant instant = Instant.ofEpochMilli(time);
                    Instant now = Instant.now();
                    if (Duration.between(instant, now).toMinutes() < 1) {
                        atomicBoolean.set(false);
                        atomicReference.set(now.plus(1, ChronoUnit.MINUTES));
                        throw new RuntimeException("Not valid");
                    }
                })
                .process(exchange -> {
                    KafkaManualCommit kafkaManualCommit = exchange.getIn().getHeader("CamelKafkaManualCommit",KafkaManualCommit.class);
                    kafkaManualCommit.commit();
                });
    }
}
