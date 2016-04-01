/**
 * Copyright (C) 2012 All rights reserved
 * VPRO The Netherlands
 */
package nl.vpro.esper.service;

import java.time.Duration;
import java.util.concurrent.*;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncEventServiceProviderImpl extends EventServiceProviderImpl implements AsyncEventServiceProvider {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncEventServiceProviderImpl.class);

    private final BlockingQueue<Object> queue;

    private ExecutorService executor;

    public AsyncEventServiceProviderImpl(int queueSize) {
        super();
        queue = new ArrayBlockingQueue<>(queueSize);
    }

    public AsyncEventServiceProviderImpl(String name) {
        this(name, 200);
    }

    public AsyncEventServiceProviderImpl(String name, int queueSize) {
        this(name, new String[] {}, queueSize);
    }

    public AsyncEventServiceProviderImpl(String name, String... eventPackage) {
        this(name, eventPackage, 200);
    }

    public AsyncEventServiceProviderImpl(String name, String eventPackage, int queueSize) {
        this(name, new String[] {eventPackage}, queueSize);
    }

    public AsyncEventServiceProviderImpl(String name, String[] eventPackage, int queueSize) {
        super(name, eventPackage);
        queue = new ArrayBlockingQueue<>(queueSize);
    }


    @PostConstruct
    private void init() {
        executor = Executors.newSingleThreadExecutor();
        executor.submit(new EventHandler());
    }

    @PreDestroy
    private void shutDown() {
        executor.shutdown();
        epServiceProvider.destroy();
        queue.clear();
    }

    @Override
    public void send(Object event) {
        try {
            queue.put(event);
        } catch(InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean offer(Object event, Duration timeout) {
        try {
            return queue.offer(event, timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException ie) {
            LOG.warn(ie.getMessage());
            return false;
        }
    }

    private class EventHandler implements Runnable {
        @Override
        public void run() {
            while(true) {
                try {
                    Object event = queue.take();
                    epRuntime.sendEvent(event);
                } catch(InterruptedException e) {
                    LOG.warn(e.getMessage());
                }
            }
        }
    }
}
