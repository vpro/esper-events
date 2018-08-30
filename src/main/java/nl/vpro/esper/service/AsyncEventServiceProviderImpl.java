/*
 * Copyright (C) 2012 All rights reserved
 * VPRO The Netherlands
 */
package nl.vpro.esper.service;

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.*;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@Slf4j
public class AsyncEventServiceProviderImpl extends EventServiceProviderImpl implements AsyncEventServiceProvider {

    private final BlockingQueue<Object> queue;

    private final ExecutorService EXECUTOR = Executors.newSingleThreadExecutor();

    private Duration defaultTimeout = Duration.ofSeconds(10);
    private boolean running = true;


    public AsyncEventServiceProviderImpl(int queueCapacity) {
        super();
        queue = new ArrayBlockingQueue<>(queueCapacity);
    }

    public AsyncEventServiceProviderImpl(String name) {
        this(name, 200);
    }

    public AsyncEventServiceProviderImpl(String name, int queueCapacity) {
        this(name, new String[] {}, queueCapacity);
    }

    public AsyncEventServiceProviderImpl(String name, String... eventPackage) {
        this(name, eventPackage, 200);
    }

    public AsyncEventServiceProviderImpl(String name, String eventPackage, int queueCapacity) {
        this(name, new String[] {eventPackage}, queueCapacity);
    }

    public AsyncEventServiceProviderImpl(String name, String[] eventPackage, int queueCapacity) {
        super(name, eventPackage);
        queue = new ArrayBlockingQueue<>(queueCapacity);
    }


    @PostConstruct
    void init() {
        EXECUTOR.submit(new EventHandler());
    }

    @PreDestroy
    private void shutDown() {
        running = false;
        epServiceProvider.destroy();
        queue.clear();
        EXECUTOR.shutdownNow();
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
            log.warn(ie.getClass().getName(), ie.getMessage());
            return false;
        }
    }

    public Duration getDefaultTimeout() {
        return defaultTimeout;
    }

    public void setDefaultTimeout(Duration defaultTimeout) {
        this.defaultTimeout = defaultTimeout;
    }


    public void setDefaultTimeoutAsString(String defaultTimeout) {
        try {
            this.defaultTimeout = Duration.parse(defaultTimeout);
        } catch(Exception e) {

        }
    }

    private class EventHandler implements Runnable {
        private Instant lastLog = Instant.EPOCH;
        @Override
        public void run() {
            while(running) {
                try {
                    if (queue.size() > queue.remainingCapacity() && lastLog.isBefore(Instant.now().minus(Duration.ofMinutes(5)))) {
                        log.warn("Queue size {} (remaining capacity {})", queue.size(), queue.remainingCapacity());
                        lastLog = Instant.now();
                    }
                    Object event = queue.poll(5, TimeUnit.SECONDS);
                    if (event != null) {
                        epRuntime.sendEvent(event);
                    }
                } catch(InterruptedException e) {
                    log.warn(e.getMessage());
                    break;
                } catch(Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
    }
}
