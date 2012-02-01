/**
 * Copyright (C) 2012 All rights reserved
 * VPRO The Netherlands
 */
package nl.vpro.esper.service;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

public class TestListener implements UpdateListener {

    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        System.out.println((newEvents[0]));
    }

}
