/*
 * Copyright (C) 2012 All rights reserved
 * VPRO The Netherlands
 */
package nl.vpro.esper.service;

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.*;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@Slf4j
public class AsyncEventServiceProviderImpl extends EventServiceProviderImpl implements AsyncEventServiceProvider {

    private final BlockingQueue<Object> queue;

    private ExecutorService executor;


    private Duration defaultTimeout = Duration.ofSeconds(10);

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
        offer(event, defaultTimeout);
    }

    @Override
    public boolean offer(Object event, Duration timeout) {
        try {
            return queue.offer(event, timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException ie) {
            log.warn(ie.getMessage());
            return false;
        }
    }

    public Duration getDefaultTimeout() {
        return defaultTimeout;
    }

    public void setDefaultTimeout(Duration defaultTimeout) {
        this.defaultTimeout = defaultTimeout;
    }

    private class EventHandler implements Runnable {
        @Override
        public void run() {
            while(true) {
                try {
                    Object event = queue.take();
                    epRuntime.sendEvent(event);
                } catch(InterruptedException e) {
                    log.warn(e.getMessage());
                }
            }
        }
    }
}
