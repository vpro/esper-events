/*
 * Copyright (C) 2012 All rights reserved
 * VPRO The Netherlands
 */
package nl.vpro.esper.listener;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import nl.vpro.esper.event.TestEvent;
import nl.vpro.esper.service.EventServiceProvider;

import static org.fest.assertions.Assertions.assertThat;

/**
 * See https://jira.vpro.nl/browse/MSE-
 *
 * @author Roelof Jan Koekoek
 * @since 1.4
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:/nl/vpro/esper/listener/topListTest-context.xml")
public class TopListTest {

    @Autowired
    private EventServiceProvider provider;

    @Autowired
    private TopList listener;

    @Test
    public void testUpdate() throws Exception {
        for(int i = 1; i <= 20; i++) {
            for(int j = 1; j <= i; j++) {
                provider.send(new TestEvent("Event " + i));
            }
        }

        assertThat(listener.getTopRatings()).hasSize(5);
        assertThat(listener.getTopRatings().get(0).getKey()).isEqualTo("Event 20");
        assertThat(listener.getTopRatings().get(0).getScore()).isEqualTo(20);
        assertThat(listener.getTopRatings().get(4).getKey()).isEqualTo("Event 16");
        assertThat(listener.getTopRatings().get(4).getScore()).isEqualTo(16);
    }
}
