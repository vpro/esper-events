/*
 * Copyright (C) 2012 All rights reserved
 * VPRO The Netherlands
 */
package nl.vpro.esper.service;

import java.time.Duration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import nl.vpro.esper.event.TestEvent;
import nl.vpro.esper.listener.Counter;

import static org.assertj.core.api.Assertions.assertThat;

public class AsyncEventServiceProviderTest {

    private AsyncEventServiceProviderImpl provider;
    private Counter listener;

    @BeforeEach
    public void setup() {
        Statement testStatement = new Statement("select count(*) from TestEvent where name like '%6'");
        provider = AsyncEventServiceProviderImpl.asyncBuilder().name("200")
            .packages("nl.vpro.esper.event")
            .build();
        provider.init();
        provider.addStatement(testStatement);
        listener = new Counter();
        testStatement.addListener(listener);
    }

    @Test
    public void testService() throws Exception {
        for(int i = 1; i <= 20; i++) {
            provider.offer(new TestEvent("Event " + i), Duration.ofMillis(1000));
        }

        Thread.sleep(10);

        assertThat(listener.getCount()).isEqualTo(2);
    }
}
