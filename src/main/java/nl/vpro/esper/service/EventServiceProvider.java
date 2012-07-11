/**
 * Copyright (C) 2012 All rights reserved
 * VPRO The Netherlands
 */
package nl.vpro.esper.service;

public interface EventServiceProvider {

    void addStatement(Statement statement);

    void send(Object event);

}
