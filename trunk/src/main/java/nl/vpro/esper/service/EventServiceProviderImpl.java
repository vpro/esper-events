/**
 * Copyright (C) 2012 All rights reserved
 * VPRO The Netherlands
 */
package nl.vpro.esper.service;

import java.util.LinkedHashSet;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.espertech.esper.client.*;

public class EventServiceProviderImpl implements EventServiceProvider {

    protected EPServiceProvider epServiceProvider;

    protected EPRuntime epRuntime;

    protected Set<Statement> statements = new LinkedHashSet<Statement>();

    public EventServiceProviderImpl() {
        Configuration config = new Configuration();
        config.addEventTypeAutoName("nl.vpro.esper.event");

        epServiceProvider = EPServiceProviderManager.getDefaultProvider(config);
        epRuntime = epServiceProvider.getEPRuntime();
    }

    public EventServiceProviderImpl(String name) {
        this(name, "nl.vpro.esper.event");
    }

    public EventServiceProviderImpl(String name, String eventPackage) {
        Configuration config = new Configuration();
        config.addEventTypeAutoName(eventPackage);

        epServiceProvider = EPServiceProviderManager.getProvider(name, config);
        epRuntime = epServiceProvider.getEPRuntime();
    }

    @PostConstruct
    private void init() {
        for(Statement statement : statements) {
            EPStatement epStatement = epServiceProvider.getEPAdministrator().createEPL(statement.getEPL());
            statement.setEPStatement(epStatement);
        }
    }

    @PreDestroy
    private void shutDown() {
        epServiceProvider.destroy();
    }

    public void send(Object event) {
        epRuntime.sendEvent(event);
    }

    public void addStatement(Statement statement) {
        this.statements.add(statement);
    }

    public void setStatements(Set<Statement> statements) {
        this.statements = statements;
    }
}
