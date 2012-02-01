/**
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

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:/nl/vpro/esper/service/eventServiceProviderTest-context.xml")
public class EventServiceProviderTest {

    @Autowired
    private EventServiceProvider provider;

    @Test
    public void testService() {
        for(int i = 1; i <= 20; i++) {
            provider.sendEvent(new TestEvent("Event " + i));
        }
    }
}
