/*
 * Copyright (C) 2012 All rights reserved
 * VPRO The Netherlands
 */
package nl.vpro.esper.service;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.lang.annotation.Annotation;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.*;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

@Slf4j
public class AsyncEventServiceProviderImpl extends EventServiceProviderImpl implements AsyncEventServiceProvider {

    private final BlockingQueue<Object> queue;

    private final ExecutorService EXECUTOR = Executors.newSingleThreadExecutor();

    @Getter
    @Setter
    private Duration defaultTimeout = Duration.ofSeconds(10);
    private boolean running = true;

    @lombok.Builder(builderMethodName = "asyncBuilder")
    private AsyncEventServiceProviderImpl(
        String name,
        Set<String> eventPackages,
        Set<Class<? extends Annotation>> eventAnnotations,
        int queueCapacity)  {
        super(name, eventPackages, eventAnnotations);
        queue = new ArrayBlockingQueue<>(queueCapacity);
    }


    @PostConstruct
    void init() {
        EXECUTOR.submit(new EventHandler());
    }

    @PreDestroy
    private void shutDown() {
        running = false;
        epRuntime.destroy();
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
            log.info("Interrupted");
            Thread.currentThread().interrupt();
            return false;
        }
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
                        epRuntime.getEventService().sendEventBean(event, event.getClass().getSimpleName());
                    }
                } catch(InterruptedException e) {
                    log.info("Interrupted");
                    Thread.currentThread().interrupt();
                    break;
                } catch(Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
    }
}
