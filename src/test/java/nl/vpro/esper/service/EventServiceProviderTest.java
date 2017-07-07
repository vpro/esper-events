/*
 * Copyright (C) 2012 All rights reserved
 * VPRO The Netherlands
 */
package nl.vpro.esper.service;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import nl.vpro.esper.event.TestEvent;
import nl.vpro.esper.listener.Counter;

import static org.assertj.core.api.Assertions.assertThat;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:/nl/vpro/esper/service/eventServiceProviderTest-context.xml")
public class EventServiceProviderTest {

    @Autowired
    private EventServiceProvider provider;

    @Autowired
    private Counter listener;

    @Test
    public void testService() throws Exception {
        for(int i = 1; i <= 20; i++) {
            provider.send(new TestEvent("Event " + i));
        }

        assertThat(listener.getCount()).isEqualTo(2);
    }
}
