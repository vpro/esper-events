/**
 * Copyright (C) 2012 All rights reserved
 * VPRO The Netherlands
 */
package nl.vpro.esper.listener;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.espertech.esper.event.map.MapEventBean;

public class Counter implements UpdateListener {

    private Long count = 0l;

    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        count = (Long)((MapEventBean)newEvents[0]).getProperties().get("count(*)");
    }

    public Long getCount() {
        return count;
    }
}
