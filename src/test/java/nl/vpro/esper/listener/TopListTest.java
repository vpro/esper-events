/*
 * Copyright (C) 2012 All rights reserved
 * VPRO The Netherlands
 */
package nl.vpro.esper.listener;


import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import nl.vpro.esper.event.TestEvent;
import nl.vpro.esper.service.*;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * See https://jira.vpro.nl/browse/MSE-
 *
 * @author Roelof Jan Koekoek
 * @since 1.4
 */
public class TopListTest {

    TopList listener;
    EventServiceProvider provider;
    @BeforeEach
    public void setup() {
        provider = new EventServiceProviderImpl("test", TestEvent.class.getPackage());
        Statement statement = new Statement("select name, count(*) from TestEvent.win:time(1 min) group by name");
        listener = new TopList("name", 5, true);
        statement.addListener(listener);
        provider.addStatement(statement);
    }


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
