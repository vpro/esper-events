/*
 * Copyright (C) 2012 All rights reserved
 * VPRO The Netherlands
 */
package nl.vpro.esper.service;

import java.time.Duration;

@SuppressWarnings("UnusedReturnValue")
public interface AsyncEventServiceProvider extends EventServiceProvider {

    boolean offer(Object event, Duration timeout);

}
