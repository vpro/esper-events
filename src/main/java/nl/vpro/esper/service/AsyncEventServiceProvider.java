/**
 * Copyright (C) 2012 All rights reserved
 * VPRO The Netherlands
 */
package nl.vpro.esper.service;

public interface AsyncEventServiceProvider extends EventServiceProvider {

    boolean offer(Object event);

}
