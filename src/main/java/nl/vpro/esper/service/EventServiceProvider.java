/*
 * Copyright (C) 2012 All rights reserved
 * VPRO The Netherlands
 */
package nl.vpro.esper.service;

public interface EventServiceProvider {

    void addStatement(Statement statement);

    void send(Object event);

    static EventServiceProviderImpl.Builder builder() {
        return EventServiceProviderImpl.builder();
    }

    static AsyncEventServiceProviderImpl.Builder async() {
        return AsyncEventServiceProviderImpl.asyncBuilder();
    }

}
