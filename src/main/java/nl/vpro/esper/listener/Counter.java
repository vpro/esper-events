/*
 * Copyright (C) 2012 All rights reserved
 * VPRO The Netherlands
 */
package nl.vpro.esper.listener;

import lombok.extern.slf4j.Slf4j;

import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.common.internal.event.map.MapEventBean;
import com.espertech.esper.runtime.client.*;


@Slf4j
public class Counter implements UpdateListener {

    private Long count = 0L;

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents, EPStatement statement, EPRuntime runtime) {
        count = (Long)((MapEventBean)newEvents[0]).getProperties().get("count(*)");
        log.debug("Count: " + count);
    }

    public Long getCount() {
        return count;
    }
}
