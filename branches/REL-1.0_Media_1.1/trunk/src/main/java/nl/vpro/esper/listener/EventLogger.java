/**
 * Copyright (C) 2012 All rights reserved
 * VPRO The Netherlands
 */
package nl.vpro.esper.listener;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.beanutils.PropertyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.espertech.esper.event.map.MapEventBean;

public class EventLogger implements UpdateListener {
    private final Logger logger;

    private final String message;

    private final String[] properties;

    public EventLogger(String category, String message, String... eventProperties) {
        logger = LoggerFactory.getLogger(category);
        this.message = message;
        this.properties = eventProperties;
    }

    @SuppressWarnings("unchecked")
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        EventBean eventBean = newEvents[0];

        List<Object> propList = null;
        if(eventBean instanceof MapEventBean) {
            propList = handleMap(eventBean);
        } else {
            Object event = eventBean.getUnderlying();
            propList = handelEvent(event);
        }

        logger.info(message, propList.toArray());
    }

    private List<Object> handleMap(EventBean map) {
        List<Object> propList = new ArrayList<Object>();
        for(String property : properties) {
            try {
                propList.add(map.get(property));
            } catch(Exception e) {
                throw new RuntimeException("Error inspecting event bean, see root cause", e);
            }
        }
        return propList;
    }

    private List<Object> handelEvent(Object event) {
        List<Object> propList = new ArrayList<Object>();
        for(String property : properties) {
            try {
                propList.add(PropertyUtils.getProperty(event, property));
            } catch(Exception e) {
                throw new RuntimeException("Error inspecting event bean, see root cause", e);
            }
        }
        return propList;
    }
}
