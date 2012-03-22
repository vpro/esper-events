/**
 * Copyright (C) 2012 All rights reserved
 * VPRO The Netherlands
 */
package nl.vpro.esper.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.espertech.esper.event.map.MapEventBean;

public class Counter implements UpdateListener {
    private static final Logger log = LoggerFactory.getLogger(Counter.class);

    private Long count = 0l;

    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        count = (Long)((MapEventBean)newEvents[0]).getProperties().get("count(*)");
        if(log.isDebugEnabled()) {
            log.debug("Count: " + count);
        }
    }

    public Long getCount() {
        return count;
    }
}
