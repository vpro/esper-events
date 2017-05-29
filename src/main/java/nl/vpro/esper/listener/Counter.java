/*
 * Copyright (C) 2012 All rights reserved
 * VPRO The Netherlands
 */
package nl.vpro.esper.listener;

import lombok.extern.slf4j.Slf4j;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.espertech.esper.event.map.MapEventBean;

@Slf4j
public class Counter implements UpdateListener {

    private Long count = 0L;

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        count = (Long)((MapEventBean)newEvents[0]).getProperties().get("count(*)");
        log.debug("Count: " + count);
    }

    public Long getCount() {
        return count;
    }
}
