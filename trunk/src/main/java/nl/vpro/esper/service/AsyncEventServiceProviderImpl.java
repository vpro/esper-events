/**
 * Copyright (C) 2012 All rights reserved
 * VPRO The Netherlands
 */
package nl.vpro.esper.service;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.espertech.esper.client.*;

public class AsyncEventServiceProviderImpl implements AsyncEventServiceProvider {

    private EPServiceProvider epServiceProvider;

    private EPRuntime epRuntime;

    private Set<Statement> statements = new LinkedHashSet<Statement>();

    private final BlockingQueue queue = new ArrayBlockingQueue(200);

    private ExecutorService executor;

    public AsyncEventServiceProviderImpl() {
        Configuration config = new Configuration();
        config.addEventTypeAutoName("nl.vpro.esper.event");

        epServiceProvider = EPServiceProviderManager.getDefaultProvider(config);
        epRuntime = epServiceProvider.getEPRuntime();
    }

    public AsyncEventServiceProviderImpl(String name) {
        this(name, "nl.vpro.esper.event");
    }

    public AsyncEventServiceProviderImpl(String name, String eventPackage) {
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

        executor = Executors.newSingleThreadExecutor();
        executor.submit(new EventHandler());
    }

    @PreDestroy
    private void shutDown() {
        executor.shutdown();
        epServiceProvider.destroy();
        queue.clear();
    }

    public void send(Object event) {
        try {
            queue.put(event);
        } catch(InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean offer(Object event) {
        return queue.offer(event);
    }

    public void addStatement(Statement statement) {
        this.statements.add(statement);

        EPStatement epStatement = epServiceProvider.getEPAdministrator().createEPL(statement.getEPL());
        statement.setEPStatement(epStatement);
    }

    public void setStatements(Set<Statement> statements) {
        this.statements = statements;
    }

    private class EventHandler implements Runnable {

        public void run() {
            try {
                Object event = queue.take();
                epRuntime.sendEvent(event);
            } catch(InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
