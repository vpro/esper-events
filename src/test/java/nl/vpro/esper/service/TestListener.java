/**
 * Copyright (C) 2012 All rights reserved
 * VPRO The Netherlands
 */
package nl.vpro.esper.service;

import org.junit.Ignore;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

@Ignore("Not a test")
public class TestListener implements UpdateListener {

    private int count = 0;

    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        System.out.println((newEvents[0].getUnderlying()));
        count++;
    }

    public int getCount() {
        return count;
    }
}
