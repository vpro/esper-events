/**
 * Copyright (C) 2012 All rights reserved
 * VPRO The Netherlands
 */
package nl.vpro.esper.service;

import java.time.Duration;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import nl.vpro.esper.event.TestEvent;
import nl.vpro.esper.listener.Counter;

import static org.fest.assertions.Assertions.assertThat;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:/nl/vpro/esper/service/asyncEventServiceProviderTest-context.xml")
public class AsyncEventServiceProviderTest {

    @Autowired
    private AsyncEventServiceProvider provider;

    @Autowired
    private Counter listener;

    @Test
    public void testService() throws Exception {
        for(int i = 1; i <= 20; i++) {
            provider.offer(new TestEvent("Event " + i), Duration.ofMillis(1000));
        }

        Thread.sleep(10);

        assertThat(listener.getCount()).isEqualTo(2);
    }
}
